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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeManager;
import io.prestosql.exchange.ExchangeSink;
import io.prestosql.exchange.ExchangeSinkInstanceHandle;
import io.prestosql.exchange.FileSystemExchangeConfig;
import io.prestosql.exchange.FileSystemExchangeManager;
import io.prestosql.exchange.FileSystemExchangeSinkHandle;
import io.prestosql.exchange.FileSystemExchangeSinkInstanceHandle;
import io.prestosql.exchange.FileSystemExchangeStats;
import io.prestosql.exchange.storage.FileSystemExchangeStorage;
import io.prestosql.exchange.storage.HetuFileSystemExchangeStorage;
import io.prestosql.execution.MarkerDataFileFactory;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.PageAssertions;
import io.prestosql.snapshot.SnapshotStateId;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.testing.TestingPagesSerdeFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHybridSpoolingBuffer
{
    private final PagesSerde serde = new TestingPagesSerdeFactory().createPagesSerde();
    private final PagesSerde javaSerde = new PagesSerdeFactory(createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(), false)
            .createDirectPagesSerde(Optional.empty(), true, false);
    private final PagesSerde kryoSerde = new PagesSerdeFactory(createTestMetadataManager().getFunctionAndTypeManager().getBlockKryoEncodingSerde(), false)
            .createDirectPagesSerde(Optional.empty(), true, true);
    private ExchangeSink exchangeSink;
    private ExchangeManager exchangeManager;
    private ExchangeSinkInstanceHandle exchangeSinkInstanceHandle;
    private final String baseURI = "file:///tmp/hetu/spooling";
    private final String baseDir = "/tmp/hetu/spooling";
    private final String accessDir = "/tmp/hetu";
    private final Path accessPath = Paths.get(accessDir);
    private final HetuFileSystemClient hetuFileSystemClient = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), accessPath);

    @BeforeMethod
    public void setUp()
            throws IOException, InterruptedException
    {
        Path basePath = Paths.get(baseDir);
        File base = new File(accessDir);
        if (base.exists()) {
            deleteDirectory(base);
        }
        Files.createDirectories(basePath);
    }

    @AfterMethod
    public void cleanUp()
    {
        File base = new File(accessDir);
        if (base.exists()) {
            deleteDirectory(base);
        }
    }

    private void setConfig(FileSystemExchangeConfig.DirectSerialisationType type)
    {
        FileSystemExchangeConfig config = new FileSystemExchangeConfig()
                .setExchangeEncryptionEnabled(false)
                .setDirectSerializationType(type)
                .setBaseDirectories(baseDir);

        FileSystemExchangeStorage exchangeStorage = new HetuFileSystemExchangeStorage();
        exchangeStorage.setFileSystemClient(hetuFileSystemClient);
        exchangeManager = new FileSystemExchangeManager(exchangeStorage, new FileSystemExchangeStats(), config);
        exchangeSinkInstanceHandle = new FileSystemExchangeSinkInstanceHandle(
                new FileSystemExchangeSinkHandle(0, Optional.empty(), false),
                config.getBaseDirectories().get(0),
                10);
        exchangeSink = exchangeManager.createSink(exchangeSinkInstanceHandle, false);
    }

    @Test
    public void testHybridSpoolingBufferWithSerializationOff()
            throws ExecutionException, InterruptedException
    {
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.OFF);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        List<SerializedPage> pages = new ArrayList<>();
        pages.add(generateSerializedPage());
        pages.add(generateSerializedPage());
        hybridSpoolingBuffer.enqueue(0, pages, null);
        ListenableFuture<BufferResult> result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        while (result.get().equals(BufferResult.emptyResults(0, false))) {
            result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        }
        List<Page> actualPages = result.get().getSerializedPages().stream().map(page -> serde.deserialize(page)).collect(Collectors.toList());
        assertEquals(actualPages.size(), 1);
        for (int pageCount = 0; pageCount < actualPages.size(); pageCount++) {
            PageAssertions.assertPageEquals(ImmutableList.of(INTEGER, INTEGER), actualPages.get(pageCount), generatePage());
        }
        result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        actualPages = result.get().getSerializedPages().stream().map(page -> serde.deserialize(page)).collect(Collectors.toList());
        assertEquals(actualPages.size(), 1);
        for (int pageCount = 0; pageCount < actualPages.size(); pageCount++) {
            PageAssertions.assertPageEquals(ImmutableList.of(INTEGER, INTEGER), actualPages.get(pageCount), generatePage());
        }
        hybridSpoolingBuffer.setNoMorePages();
    }

    @Test
    public void testHybridSpoolingBufferWithSerializationJava()
            throws ExecutionException, InterruptedException
    {
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.JAVA);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        hybridSpoolingBuffer.setSerde(serde);
        hybridSpoolingBuffer.setJavaSerde(javaSerde);
        hybridSpoolingBuffer.setKryoSerde(kryoSerde);
        List<Page> pages = new ArrayList<>();
        pages.add(generatePage());
        pages.add(generatePage());
        hybridSpoolingBuffer.enqueuePages(0, pages, null, javaSerde);
        ListenableFuture<BufferResult> result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        while (result.get().equals(BufferResult.emptyResults(0, false))) {
            result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        }
        List<Page> actualPages = result.get().getSerializedPages().stream().map(page -> serde.deserialize(page)).collect(Collectors.toList());
        assertEquals(actualPages.size(), 1);
        for (int pageCount = 0; pageCount < actualPages.size(); pageCount++) {
            PageAssertions.assertPageEquals(ImmutableList.of(INTEGER, INTEGER), actualPages.get(pageCount), generatePage());
        }
        result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        actualPages = result.get().getSerializedPages().stream().map(page -> serde.deserialize(page)).collect(Collectors.toList());
        assertEquals(actualPages.size(), 1);
        for (int pageCount = 0; pageCount < actualPages.size(); pageCount++) {
            PageAssertions.assertPageEquals(ImmutableList.of(INTEGER, INTEGER), actualPages.get(pageCount), generatePage());
        }
        hybridSpoolingBuffer.setNoMorePages();
    }

    @Test
    public void testHybridSpoolingBufferWithSerializationKryo()
            throws ExecutionException, InterruptedException
    {
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.KRYO);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        hybridSpoolingBuffer.setSerde(serde);
        hybridSpoolingBuffer.setJavaSerde(javaSerde);
        hybridSpoolingBuffer.setKryoSerde(kryoSerde);
        List<Page> pages = new ArrayList<>();
        pages.add(generatePage());
        pages.add(generatePage());
        hybridSpoolingBuffer.enqueuePages(0, pages, null, kryoSerde);
        ListenableFuture<BufferResult> result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        while (result.get().equals(BufferResult.emptyResults(0, false))) {
            result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        }
        List<Page> actualPages = result.get().getSerializedPages().stream().map(page -> serde.deserialize(page)).collect(Collectors.toList());
        assertEquals(actualPages.size(), 1);
        for (int pageCount = 0; pageCount < actualPages.size(); pageCount++) {
            PageAssertions.assertPageEquals(ImmutableList.of(INTEGER, INTEGER), actualPages.get(pageCount), generatePage());
        }
        result = hybridSpoolingBuffer.get(new OutputBuffers.OutputBufferId(0), 0, new DataSize(100, DataSize.Unit.MEGABYTE));
        actualPages = result.get().getSerializedPages().stream().map(page -> serde.deserialize(page)).collect(Collectors.toList());
        assertEquals(actualPages.size(), 1);
        for (int pageCount = 0; pageCount < actualPages.size(); pageCount++) {
            PageAssertions.assertPageEquals(ImmutableList.of(INTEGER, INTEGER), actualPages.get(pageCount), generatePage());
        }
        hybridSpoolingBuffer.setNoMorePages();
    }

    @Test
    public void testHybridSpoolingMarkerIndexFile()
    {
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.OFF);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        URI markerDataFile = URI.create(baseURI).resolve("marker_data_file.data");
        URI spoolingDataFile = URI.create(baseURI).resolve("spooling_data_file.data");
        HybridSpoolingBuffer.SpoolingInfo spoolingInfo = new HybridSpoolingBuffer.SpoolingInfo(1000, 2000);
        hybridSpoolingBuffer.enqueueMarkerIndex(1, markerDataFile, 1000, 2000, ImmutableMap.of(spoolingDataFile, spoolingInfo));
    }

    @Test
    public void testHybridSpoolingMarkerDataFile()
    {
        SnapshotStateId snapshotStateId = new SnapshotStateId(1, new TaskId(new StageId("query", 1), 1, 1));
        SnapshotStateId snapshotStateId1 = new SnapshotStateId(2, new TaskId(new StageId("query", 1), 1, 1));
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.OFF);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        hybridSpoolingBuffer.enqueueMarkerData(1, ImmutableMap.of(snapshotStateId, "marker1"), exchangeSink.getSinkFiles());
        hybridSpoolingBuffer.enqueueMarkerData(2, ImmutableMap.of(snapshotStateId1, "marker2"), exchangeSink.getSinkFiles());
        MarkerDataFileFactory.MarkerDataFileFooter footer = (MarkerDataFileFactory.MarkerDataFileFooter) hybridSpoolingBuffer.getMarkerDataFileFooter();
        Map<String, MarkerDataFileFactory.OperatorStateInfo> operatorStateInfoMap = footer.getOperatorStateInfo();
        MarkerDataFileFactory.OperatorStateInfo operatorStateInfo = operatorStateInfoMap.get(snapshotStateId1.getId());
        assertEquals(hybridSpoolingBuffer.getMarkerData(operatorStateInfo.getStateOffset()), "marker2");
        MarkerDataFileFactory.MarkerDataFileFooter previousFooter = (MarkerDataFileFactory.MarkerDataFileFooter) hybridSpoolingBuffer.getMarkerDataFileFooter(footer.getPreviousTailOffset());
        Map<String, MarkerDataFileFactory.OperatorStateInfo> previousOperatorStateInfoMap = previousFooter.getOperatorStateInfo();
        MarkerDataFileFactory.OperatorStateInfo previousOperatorStateInfo = previousOperatorStateInfoMap.get(snapshotStateId.getId());
        assertEquals(hybridSpoolingBuffer.getMarkerData(previousOperatorStateInfo.getStateOffset()), "marker1");
    }

    @Test
    public void testMarkerSpoolingInfo()
    {
        int entries = 10;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);
        SerializedPage serializedPage = javaSerde.serialize(page);
        SnapshotStateId snapshotStateId = new SnapshotStateId(1, new TaskId(new StageId("query", 1), 1, 1));
        SnapshotStateId snapshotStateId1 = new SnapshotStateId(1, new TaskId(new StageId("query", 1), 2, 1));
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.OFF);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        hybridSpoolingBuffer.enqueue(0, ImmutableList.of(serializedPage), null);
        hybridSpoolingBuffer.enqueueMarkerInfo(1, ImmutableMap.of(snapshotStateId, "marker1", snapshotStateId1, "marker2"));
        assertEquals(hybridSpoolingBuffer.dequeueMarkerInfo(1), ImmutableMap.of(snapshotStateId, "marker1", snapshotStateId1, "marker2"));
    }

    @Test
    public void testMarkerSpoolingInfoMultiPartition()
    {
        int entries = 10;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);
        SerializedPage serializedPage = javaSerde.serialize(page);
        SnapshotStateId snapshotStateId = new SnapshotStateId(1, new TaskId(new StageId("query", 1), 1, 1));
        SnapshotStateId snapshotStateId1 = new SnapshotStateId(1, new TaskId(new StageId("query", 1), 2, 1));
        setConfig(FileSystemExchangeConfig.DirectSerialisationType.OFF);
        HybridSpoolingBuffer hybridSpoolingBuffer = createHybridSpoolingBuffer();
        hybridSpoolingBuffer.enqueue(0, ImmutableList.of(serializedPage), null);
        hybridSpoolingBuffer.enqueue(1, ImmutableList.of(serializedPage), null);
        hybridSpoolingBuffer.enqueueMarkerInfo(1, ImmutableMap.of(snapshotStateId, "marker1"));
        hybridSpoolingBuffer.enqueueMarkerInfo(2, ImmutableMap.of(snapshotStateId1, "marker2"));
        assertEquals(hybridSpoolingBuffer.dequeueMarkerInfo(1), ImmutableMap.of(snapshotStateId, "marker1"));
        assertEquals(hybridSpoolingBuffer.dequeueMarkerInfo(2), ImmutableMap.of(snapshotStateId1, "marker2"));
    }

    private HybridSpoolingBuffer createHybridSpoolingBuffer()
    {
        OutputBuffers outputBuffers = OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.PARTITIONED);
        outputBuffers.setExchangeSinkInstanceHandle(exchangeSinkInstanceHandle);

        return new HybridSpoolingBuffer(new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), directExecutor()),
                outputBuffers,
                exchangeSink,
                TestSpoolingExchangeOutputBuffer.TestingLocalMemoryContext::new,
                exchangeManager);
    }

    private SerializedPage generateSerializedPage()
    {
        Page expectedPage = generatePage();
        SerializedPage page = serde.serialize(expectedPage);
        return page;
    }

    private Page generatePage()
    {
        BlockBuilder expectedBlockBuilder = INTEGER.createBlockBuilder(null, 2);
        INTEGER.writeLong(expectedBlockBuilder, 10);
        INTEGER.writeLong(expectedBlockBuilder, 20);
        Block expectedBlock = expectedBlockBuilder.build();

        return new Page(expectedBlock, expectedBlock);
    }

    private boolean deleteDirectory(File dir)
    {
        File[] allContents = dir.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return dir.delete();
    }
}
