/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeManager;
import io.prestosql.exchange.ExchangeSink;
import io.prestosql.exchange.ExchangeSource;
import io.prestosql.exchange.ExchangeSourceHandle;
import io.prestosql.exchange.FileStatus;
import io.prestosql.exchange.FileSystemExchangeConfig;
import io.prestosql.exchange.FileSystemExchangeSourceHandle;
import io.prestosql.exchange.storage.FileSystemExchangeStorage;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;

import javax.crypto.SecretKey;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.exchange.FileSystemExchangeSink.DATA_FILE_SUFFIX;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HybridSpoolingBuffer
        extends SpoolingExchangeOutputBuffer
{
    private static final String PARENT_URI = "..";
    private static final Logger LOG = Logger.get(HybridSpoolingBuffer.class);

    private final OutputBufferStateMachine stateMachine;
    private final OutputBuffers outputBuffers;
    private final ExchangeSink exchangeSink;
    private ExchangeSource exchangeSource;
    private final Supplier<LocalMemoryContext> memoryContextSupplier;
    private final ExecutorService executor;
    private final ExchangeManager exchangeManager;
    private int token;
    private PagesSerde serde;
    private PagesSerde javaSerde;
    private PagesSerde kryoSerde;

    private URI outputDirectory;

    public HybridSpoolingBuffer(OutputBufferStateMachine stateMachine, OutputBuffers outputBuffers, ExchangeSink exchangeSink, Supplier<LocalMemoryContext> memoryContextSupplier, ExchangeManager exchangeManager)
    {
        super(stateMachine, outputBuffers, exchangeSink, memoryContextSupplier);
        this.stateMachine = stateMachine;
        this.outputBuffers = outputBuffers;
        this.exchangeSink = exchangeSink;
        this.memoryContextSupplier = memoryContextSupplier;
        this.outputDirectory = exchangeSink.getOutputDirectory().resolve(PARENT_URI);
        this.executor = newCachedThreadPool(daemonThreadsNamed("exchange-source-handles-creation-%s"));
        this.exchangeManager = exchangeManager;
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
    {
        if (exchangeSource == null) {
            exchangeSource = createExchangeSource(exchangeSink.getPartitionId());
            if (exchangeSource == null) {
                return immediateFuture(BufferResult.emptyResults(token, false));
            }
        }
        return immediateFuture(readPages(token));
    }

    private BufferResult readPages(long tokenId)
    {
        List<SerializedPage> result = new ArrayList<>();
        FileSystemExchangeConfig.DirectSerialisationType directSerialisationType = exchangeSource.getDirectSerialisationType();
        if (directSerialisationType != FileSystemExchangeConfig.DirectSerialisationType.OFF) {
            PagesSerde pagesSerde = (directSerialisationType == FileSystemExchangeConfig.DirectSerialisationType.JAVA) ? javaSerde : kryoSerde;
            while (token < tokenId) {
                exchangeSource.readPage(pagesSerde);
                token++;
            }
            Page page = exchangeSource.readPage(pagesSerde);
            result.add(serde.serialize(page));
        }
        else {
            while (token < tokenId) {
                exchangeSource.read();
                token++;
            }
            Slice slice = exchangeSource.read();
            SerializedPage serializedPage = slice != null ? PagesSerdeUtil.readSerializedPage(slice) : null;
            result.add(serializedPage);
        }
        return new BufferResult(tokenId, tokenId + result.size(), false, result);
    }

    public ExchangeSource createExchangeSource(int partitionId)
    {
        ExchangeHandleInfo exchangeHandleInfo = getExchangeHandleInfo(exchangeSink);
        ListenableFuture<List<FileStatus>> fileStatus = getFileStatus(outputDirectory);
        List<FileStatus> completedFileStatus = new ArrayList<>();
        if (!fileStatus.isDone()) {
            return null;
        }
        try {
            completedFileStatus = fileStatus.get();
        }
        catch (Exception e) {
            LOG.debug("Failed in creating exchange source with outputDirectory" + outputDirectory);
        }
        List<ExchangeSourceHandle> handles = ImmutableList.of(new FileSystemExchangeSourceHandle(partitionId,
                completedFileStatus, exchangeHandleInfo.getSecretKey(), exchangeHandleInfo.isExchangeCompressionEnabled()));

        return exchangeManager.createSource(handles);
    }

    private ListenableFuture<List<FileStatus>> getFileStatus(URI path)
    {
        FileSystemExchangeStorage exchangeStorage = exchangeSink.getExchangeStorage();
        return Futures.transform(exchangeStorage.listFilesRecursively(path),
                sinkOutputFiles -> sinkOutputFiles.stream().filter(file -> file.getFilePath().endsWith(DATA_FILE_SUFFIX)).collect(toImmutableList()),
                executor);
    }

    private ExchangeHandleInfo getExchangeHandleInfo(ExchangeSink exchangeSink)
    {
        return new ExchangeHandleInfo(exchangeSink.getOutputDirectory(),
                exchangeSink.getExchangeStorage(),
                exchangeSink.getSecretKey().map(SecretKey::getEncoded),
                exchangeSink.isExchangeCompressionEnabled());
    }

    @Override
    public void setJavaSerde(PagesSerde javaSerde)
    {
        this.javaSerde = javaSerde;
    }

    @Override
    public void setKryoSerde(PagesSerde kryoSerde)
    {
        this.kryoSerde = kryoSerde;
    }

    @Override
    public void setSerde(PagesSerde serde)
    {
        this.serde = serde;
    }

    private static class ExchangeHandleInfo
    {
        URI outputDirectory;
        FileSystemExchangeStorage exchangeStorage;
        Optional<byte[]> secretKey;
        boolean exchangeCompressionEnabled;

        ExchangeHandleInfo(URI outputDirectory, FileSystemExchangeStorage exchangeStorage, Optional<byte[]> secretKey, boolean exchangeCompressionEnabled)
        {
            this.outputDirectory = outputDirectory;
            this.exchangeStorage = exchangeStorage;
            this.secretKey = secretKey;
            this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        }

        public URI getOutputDirectory()
        {
            return outputDirectory;
        }

        public Optional<byte[]> getSecretKey()
        {
            return secretKey;
        }

        public boolean isExchangeCompressionEnabled()
        {
            return exchangeCompressionEnabled;
        }

        public FileSystemExchangeStorage getExchangeStorage()
        {
            return exchangeStorage;
        }
    }
}
