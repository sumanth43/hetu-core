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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
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
import io.prestosql.execution.MarkerDataFileFactory;
import io.prestosql.execution.MarkerIndexFileFactory;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.snapshot.RecoveryUtils;
import io.prestosql.snapshot.SnapshotStateId;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.exchange.FileSystemExchangeSink.DATA_FILE_SUFFIX;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.filesystem.SupportedFileAttributes.SIZE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HybridSpoolingBuffer
        extends SpoolingExchangeOutputBuffer
{
    private static final String PARENT_URI = "..";
    private static final String MARKER_INDEX_FILE = "marker_index.json";
    private static final String MARKER_DATA_FILE = "marker_data_file.data";
    private static final String TEMP_FILE = "marker_temp_txn.data";
    private static final Logger LOG = Logger.get(HybridSpoolingBuffer.class);

    private final OutputBufferStateMachine stateMachine;
    private final OutputBuffers outputBuffers;
    private final ExchangeSink exchangeSink;
    private ExchangeSource exchangeSource;
    private final Supplier<LocalMemoryContext> memoryContextSupplier;
    private final ExecutorService executor;
    private final ExchangeManager exchangeManager;
    private MarkerIndexFileFactory.MarkerIndexFileWriter markerIndexFileWriter;
    private MarkerDataFileFactory.MarkerDataFileWriter markerDataFileWriter;
    private long previousFooterOffset;
    private long previousFooterSize;
    private int previousSuccessfulMarkerId;
    private Map<URI, SpoolingInfo> spoolingInfoMap = new HashMap<>();
    private final HetuFileSystemClient fsClient;
    private int token;
    private PagesSerde serde;
    private PagesSerde javaSerde;
    private PagesSerde kryoSerde;

    private URI outputDirectory;
    private Map<Integer, AtomicLong> writeTokenPerPartition = new HashMap<>();
    private final ListeningExecutorService spoolingExecutorService;

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
        this.fsClient = exchangeSink.getExchangeStorage().getFileSystemClient();
        this.spoolingExecutorService = listeningDecorator(newFixedThreadPool(1, daemonThreadsNamed("spooling-thread-s%")));
    }

    @Override
    public void enqueue(List<SerializedPage> pages, String origin)
    {
        enqueue(0, pages, origin);
    }

    @Override
    public void enqueue(int partition, List<SerializedPage> pages, String origin)
    {
        enqueueImpl(partition, pages, origin);
    }

    public long getWriteToken(int partition)
    {
        if (writeTokenPerPartition.containsKey(partition)) {
            return writeTokenPerPartition.get(partition).get();
        }
        else {
            return -1;
        }
    }

    private void enqueueImpl(int partition, List<SerializedPage> localPages, String origin)
    {
        List<SerializedPage> spoolPages = new LinkedList<>(localPages);
        ListenableFuture<?> future = spoolingExecutorService.submit(() -> super.enqueue(partition, spoolPages, origin));
        Futures.addCallback(future, new FutureCallback<Object>() {

            @Override
            public void onSuccess(@Nullable Object result)
            {
                if (!writeTokenPerPartition.containsKey(partition)) {
                    writeTokenPerPartition.put(partition, new AtomicLong());
                }
                writeTokenPerPartition.get(partition).getAndAdd(spoolPages.size());
            }

            @Override
            public void onFailure(Throwable t)
            {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "spooling failed");
            }
        }, directExecutor());
    }

    public void enqueueMarkerInfo(int markerId, Map<SnapshotStateId, Object> states)
    {
        createTempTransactionFile(); // this is to ensure transaction is successful. Deleted at end.
        List<URI> exchangeSinkFiles = exchangeSink.getSinkFiles();
        MarkerDataFileFactory.MarkerDataFileFooterInfo markerDataFileFooterInfo = enqueueMarkerData(markerId, states, exchangeSinkFiles);
        Map<URI, SpoolingInfo> spoolingFileInfo = new HashMap<>();
        try {
            spoolingFileInfo.putAll(createSpoolingInfo(exchangeSinkFiles));
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed creating spooling Info");
        }

        // write marker metadata to spooling file
        enqueueMarkerDataToSpoolingFile(markerDataFileFooterInfo);
        try {
            updateSpoolingInfo(exchangeSinkFiles);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed updating spooling Info");
        }
        enqueueMarkerIndex(markerId, outputDirectory.resolve(MARKER_DATA_FILE), previousFooterOffset, previousFooterSize, spoolingFileInfo);
        deleteTempTransactionFile();
        previousSuccessfulMarkerId = markerId;
    }

    public Map<SnapshotStateId, Object> dequeueMarkerInfo(int markerId)
    {
        if (checkIfTransactionFileExists()) {
            //todo(SURYA): add logic to get recent successful marker and offsets
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "transaction file exists for marker: " + markerId);
        }
        MarkerIndexFileFactory.MarkerIndexFile markerIndexFile = dequeueMarkerIndex(markerId, outputDirectory.resolve(MARKER_INDEX_FILE));
        Map<SnapshotStateId, Object> markerData = dequeueMarkerData(markerIndexFile);
        Map<URI, SpoolingInfo> spoolingInfoMap = dequeueSpoolingInfo(markerIndexFile);
        return markerData;
    }

    private boolean checkIfTransactionFileExists()
    {
        Path tempTransactionFile = Paths.get(outputDirectory.resolve(TEMP_FILE).getPath());
        return fsClient.exists(tempTransactionFile);
    }

    private void updateSpoolingInfo(List<URI> exchangeSinkFiles)
            throws IOException
    {
        for (URI sinkFile : exchangeSinkFiles) {
            if (!spoolingInfoMap.containsKey(sinkFile)) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "sink file suppose to be present in spoolingInfoMap");
            }
            else {
                long spoolingOffset = spoolingInfoMap.get(sinkFile).getSpoolingFileOffset() + spoolingInfoMap.get(sinkFile).getSpoolingFileSize();
                long spoolingSize = ((Long) fsClient.getAttribute(Paths.get(sinkFile.getPath()), SIZE)) - spoolingOffset;
                spoolingInfoMap.put(sinkFile, new SpoolingInfo(spoolingOffset, spoolingSize));
            }
        }
    }

    private void enqueueMarkerDataToSpoolingFile(MarkerDataFileFactory.MarkerDataFileFooterInfo markerDataFileFooterInfo)
    {
        exchangeSink.enqueueMarkerInfo(markerDataFileFooterInfo);
    }

    private Map<URI, SpoolingInfo> createSpoolingInfo(List<URI> sinkFiles)
            throws IOException
    {
        Map<URI, SpoolingInfo> localSpoolingInfoMap = new HashMap<>();
        for (URI sinkFile : sinkFiles) {
            if (!spoolingInfoMap.containsKey(sinkFile)) {
                localSpoolingInfoMap.put(sinkFile, new SpoolingInfo(0, (Long) fsClient.getAttribute(Paths.get(sinkFile.getPath()), SIZE)));
                spoolingInfoMap.put(sinkFile, new SpoolingInfo(0, (Long) fsClient.getAttribute(Paths.get(sinkFile.getPath()), SIZE)));
            }
            else {
                long spoolingOffset = spoolingInfoMap.get(sinkFile).getSpoolingFileOffset() + spoolingInfoMap.get(sinkFile).getSpoolingFileSize();
                long spoolingSize = ((Long) fsClient.getAttribute(Paths.get(sinkFile.getPath()), SIZE)) - spoolingOffset;
                if (spoolingSize > 0) {
                    localSpoolingInfoMap.put(sinkFile, new SpoolingInfo(spoolingOffset, spoolingSize));
                    spoolingInfoMap.put(sinkFile, new SpoolingInfo(spoolingOffset, spoolingSize));
                }
            }
        }
        return localSpoolingInfoMap;
    }

    public void createTempTransactionFile()
    {
        Path tempTransactionFile = Paths.get(outputDirectory.resolve(TEMP_FILE).getPath());
        try (OutputStream outputStream = fsClient.newOutputStream(tempTransactionFile)) {
            RecoveryUtils.serializeState(previousSuccessfulMarkerId, outputStream, false);
            RecoveryUtils.serializeState(previousFooterOffset, outputStream, false);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteTempTransactionFile()
    {
        Path tempTransactionFile = Paths.get(outputDirectory.resolve(TEMP_FILE).getPath());
        try {
            fsClient.delete(tempTransactionFile);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MarkerDataFileFactory.MarkerDataFileFooterInfo enqueueMarkerData(int markerId, Map<SnapshotStateId, Object> statesMap, List<URI> sinkFiles)
    {
        if (markerDataFileWriter == null) {
            MarkerDataFileFactory markerDataFileFactory = new MarkerDataFileFactory(outputDirectory, fsClient);
            this.markerDataFileWriter = markerDataFileFactory.createWriter(MARKER_DATA_FILE, false); //todo(SURYA): add JAVA/KRYO config based decision.
        }
        MarkerDataFileFactory.MarkerDataFileFooterInfo markerDataFileFooterInfo = markerDataFileWriter.writeDataFile(markerId, statesMap, previousFooterOffset, previousFooterSize);
        previousFooterOffset = markerDataFileFooterInfo.getFooterOffset();
        previousFooterSize = markerDataFileFooterInfo.getFooterSize();
        return markerDataFileFooterInfo;
    }

    public Map<SnapshotStateId, Object> dequeueMarkerData(MarkerIndexFileFactory.MarkerIndexFile markerIndexFile)
    {
        MarkerDataFileFactory.MarkerDataFileReader markerDataFileReader = new MarkerDataFileFactory.MarkerDataFileReader(fsClient, markerIndexFile.getMarkerDataFile(), false);
        return markerDataFileReader.readDataFile(markerIndexFile.getMarkerStartOffset(), markerIndexFile.getMarkerLength());
    }

    public Map<URI, SpoolingInfo> dequeueSpoolingInfo(MarkerIndexFileFactory.MarkerIndexFile markerIndexFile)
    {
        return markerIndexFile.getSpoolingInfoMap();
    }

    public void enqueueMarkerIndex(int markerId, URI markerDataFile, long markerStartOffset, long markerLength, Map<URI, SpoolingInfo> spoolingInfoMap)
    {
        if (markerIndexFileWriter == null) {
            MarkerIndexFileFactory markerIndexFileWriterFactory = new MarkerIndexFileFactory(outputDirectory, fsClient);
            this.markerIndexFileWriter = markerIndexFileWriterFactory.createWriter(MARKER_INDEX_FILE);
        }
        MarkerIndexFileFactory.MarkerIndexFile markerIndexFile = new MarkerIndexFileFactory.MarkerIndexFile(markerId, markerDataFile, markerStartOffset, markerLength, spoolingInfoMap);
        markerIndexFileWriter.writeIndexFile(markerIndexFile);
    }

    private MarkerIndexFileFactory.MarkerIndexFile dequeueMarkerIndex(int markerId, URI indexFile)
    {
        MarkerIndexFileFactory.MarkerIndexFileReader markerIndexFileReader = new MarkerIndexFileFactory.MarkerIndexFileReader(fsClient, Paths.get(indexFile.getPath()));
        return markerIndexFileReader.readIndexFile(markerId);
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

    public static class SpoolingInfo
            implements Serializable
    {
        private long spoolingFileOffset;
        private long spoolingFileSize;

        @JsonCreator
        public SpoolingInfo(
                @JsonProperty("spoolingFileOffset") long spoolingFileOffset,
                @JsonProperty("spoolingFileSize") long spoolingFileSize)
        {
            this.spoolingFileOffset = spoolingFileOffset;
            this.spoolingFileSize = spoolingFileSize;
        }

        @JsonProperty
        public long getSpoolingFileOffset()
        {
            return spoolingFileOffset;
        }

        @JsonProperty
        public long getSpoolingFileSize()
        {
            return spoolingFileSize;
        }
    }

    public Object getMarkerDataFileFooter()
    {
        try {
            InputStream inputStream = fsClient.newInputStream(Paths.get(outputDirectory.resolve(MARKER_DATA_FILE).getPath()));
            inputStream.skip(previousFooterOffset);
            Object o = RecoveryUtils.deserializeState(inputStream, false);
            return o;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Object getMarkerDataFileFooter(long previousFooterOffset)
    {
        try {
            InputStream inputStream = fsClient.newInputStream(Paths.get(outputDirectory.resolve(MARKER_DATA_FILE).getPath()));
            inputStream.skip(previousFooterOffset);
            Object o = RecoveryUtils.deserializeState(inputStream, false);
            return o;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Object getMarkerData(long stateOffset)
    {
        try {
            InputStream inputStream = fsClient.newInputStream(Paths.get(outputDirectory.resolve(MARKER_DATA_FILE).getPath()));
            inputStream.skip(stateOffset);
            Object o = RecoveryUtils.deserializeState(inputStream, false);
            return o;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
