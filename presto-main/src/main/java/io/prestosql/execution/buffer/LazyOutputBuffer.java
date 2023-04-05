/*
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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ExtendedSettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.Session;
import io.prestosql.exchange.ExchangeManager;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.exchange.ExchangeSink;
import io.prestosql.exchange.ExchangeSinkInstanceHandle;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.SystemSessionProperties.getRetryPolicy;
import static io.prestosql.SystemSessionProperties.isTaskAsyncParallelWriteEnabled;
import static io.prestosql.execution.buffer.BufferResult.emptyResults;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static java.util.Objects.requireNonNull;

public class LazyOutputBuffer
        implements OutputBuffer
{
    private static final Logger LOG = Logger.get(LazyOutputBuffer.class);
    private final OutputBufferStateMachine stateMachine;

    private final DataSize maxBufferSize;
    private final Supplier<LocalMemoryContext> systemMemoryContextSupplier;
    private final Executor executor;

    @GuardedBy("this")
    private OutputBuffer delegate;

    @GuardedBy("this")
    private OutputBuffer hybridSpoolingDelegate;

    @GuardedBy("this")
    private final Set<OutputBufferId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();

    @GuardedBy("this")
    private Map<Integer, Long> partitionedTokenIds = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private AtomicLong bufferSize = new AtomicLong();

    @GuardedBy("this")
    private Map<Integer, ConcurrentLinkedQueue<SerializedPage>> serializedPartitionedPages = new ConcurrentHashMap<>();

    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private Optional<ExchangeSink> exchangeSink;

    private long tokenRemoved = 0;
    private boolean noMorePages;
    private boolean isTaskAsyncParallelWriteEnabled;

    private PagesSerde serde;
    private PagesSerde javaSerde;
    private PagesSerde kryoSerde;

    public LazyOutputBuffer(
            TaskId taskId,
            Executor executor,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        requireNonNull(taskId, "taskId is null");
        this.executor = requireNonNull(executor, "executor is null");
        stateMachine = new OutputBufferStateMachine(taskId, executor);
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.systemMemoryContextSupplier = requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return stateMachine.getState() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        // until output buffer is initialized, it is "full"
        if (outputBuffer == null) {
            return 1.0;
        }
        return outputBuffer.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        // until output buffer is initialized, readers cannot enqueue and thus cannot be blocked
        return (outputBuffer != null) && outputBuffer.isOverutilized();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        if (outputBuffer == null) {
            //
            // NOTE: this code must be lock free to not hanging state machine updates
            //
            BufferState bufferState = this.stateMachine.getState();

            return new OutputBufferInfo(
                    "UNINITIALIZED",
                    bufferState,
                    bufferState.canAddBuffers(),
                    bufferState.canAddPages(),
                    0,
                    0,
                    0,
                    0,
                    ImmutableList.of());
        }
        return outputBuffer.getInfo();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        Set<OutputBufferId> abortedBuffersIds = ImmutableSet.of();
        List<PendingRead> bufferPendingReads = ImmutableList.of();
        OutputBuffer outputBuffer;
        ExchangeManager exchangeManager = null;
        synchronized (this) {
            if (delegate == null) {
                // ignore set output if buffer was already destroyed or failed
                if (stateMachine.getState().isTerminal()) {
                    return;
                }
                switch (newOutputBuffers.getType()) {
                    case PARTITIONED:
                        delegate = new PartitionedOutputBuffer(stateMachine, newOutputBuffers, maxBufferSize, systemMemoryContextSupplier, executor, isTaskAsyncParallelWriteEnabled);
                        break;
                    case BROADCAST:
                        isTaskAsyncParallelWriteEnabled = false;
                        delegate = new BroadcastOutputBuffer(stateMachine, maxBufferSize, systemMemoryContextSupplier, executor);
                        break;
                    case ARBITRARY:
                        delegate = new ArbitraryOutputBuffer(stateMachine, maxBufferSize, systemMemoryContextSupplier, executor, isTaskAsyncParallelWriteEnabled);
                        break;
                    default:
                        //TODO(Alex): decide the spool output buffer
                        LOG.info("Unexpected output buffer type: " + newOutputBuffers.getType());
                }

                if (newOutputBuffers.getExchangeSinkInstanceHandle().isPresent()) {
                    ExchangeSinkInstanceHandle exchangeSinkInstanceHandle = newOutputBuffers.getExchangeSinkInstanceHandle()
                            .orElseThrow(() -> new IllegalArgumentException("exchange sink handle is expected to be present for buffer type EXTERNAL"));
                    exchangeManager = exchangeManagerRegistry.getExchangeManager();
                    ExchangeSink exchangeSinkInstance = exchangeManager.createSink(exchangeSinkInstanceHandle, false); //TODO: create directories
                    this.exchangeSink = Optional.ofNullable(exchangeSinkInstance);
                }
                else {
                    this.exchangeSink = Optional.empty();
                }

                // process pending aborts and reads outside of synchronized lock
                abortedBuffersIds = ImmutableSet.copyOf(this.abortedBuffers);
                this.abortedBuffers.clear();
                bufferPendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            ExchangeManager finalExchangeManager = exchangeManager;
            this.exchangeSink.ifPresent(sink -> {
                if (delegate == null) {
                    delegate = new SpoolingExchangeOutputBuffer(
                            stateMachine,
                            newOutputBuffers,
                            sink,
                            systemMemoryContextSupplier);
                }
                else {
                    if (hybridSpoolingDelegate == null) {
                        hybridSpoolingDelegate = new HybridSpoolingBuffer(stateMachine,
                                newOutputBuffers,
                                sink,
                                systemMemoryContextSupplier,
                                finalExchangeManager);
                        if (hybridSpoolingDelegate != null) {
                            hybridSpoolingDelegate.setSerde(serde);
                            hybridSpoolingDelegate.setJavaSerde(javaSerde);
                            hybridSpoolingDelegate.setKryoSerde(kryoSerde);
                        }
                    }
                }
            });
            outputBuffer = delegate;
        }

        outputBuffer.setOutputBuffers(newOutputBuffers);

        // process pending aborts and reads outside of synchronized lock
        abortedBuffersIds.forEach(outputBuffer::abort);
        for (PendingRead pendingRead : bufferPendingReads) {
            pendingRead.process(outputBuffer);
        }
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                if (stateMachine.getState() == FINISHED) {
                    return immediateFuture(emptyResults(0, true));
                }

                PendingRead pendingRead = new PendingRead(bufferId, token, maxSize);
                pendingReads.add(pendingRead);
                return pendingRead.getFutureResult();
            }
            outputBuffer = delegate;
        }
        if (noMorePages && isTaskAsyncParallelWriteEnabled && hybridSpoolingDelegate != null && delegate != null && !(delegate instanceof BroadcastOutputBuffer)) {
            if (serializedPartitionedPages.containsKey(bufferId.getId()) && serializedPartitionedPages.get(bufferId.getId()).size() == 0) {
                serializedPartitionedPages.remove(bufferId.getId());
            }
            if (serializedPartitionedPages.size() != 0) {
                long tokenAcknowledged = hybridSpoolingDelegate.getWriteToken(bufferId.getId()) >= 0 ? hybridSpoolingDelegate.getWriteToken(bufferId.getId()) : tokenRemoved;
                for (int count = 0; serializedPartitionedPages.containsKey(bufferId.getId()) && partitionedTokenIds.containsKey(bufferId.getId()) && count < (tokenAcknowledged - partitionedTokenIds.get(bufferId.getId())); count++) {
                    SerializedPage serializedPage = serializedPartitionedPages.get(bufferId.getId()).poll();
                    bufferSize.getAndAdd(-serializedPage.getSizeInBytes());
                    if (serializedPartitionedPages.get(bufferId.getId()).size() == 0) {
                        serializedPartitionedPages.remove(bufferId.getId());
                    }
                }
                partitionedTokenIds.put(bufferId.getId(), tokenAcknowledged);
            }
            else {
                hybridSpoolingDelegate.setNoMorePages();
                outputBuffer.setNoMorePages();
            }
        }
        return outputBuffer.get(bufferId, token, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long token)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        if (hybridSpoolingDelegate != null && isTaskAsyncParallelWriteEnabled && delegate != null && !(delegate instanceof  BroadcastOutputBuffer)) {
            boolean clientAcknowledged = outputBuffer.checkIfAcknowledged(bufferId.getId(), token);
            if (outputBuffer instanceof ArbitraryOutputBuffer) {
                if (!clientAcknowledged) {
                    long tokenAcknowledged = Math.min(outputBuffer.getTokenId(bufferId.getId(), token), hybridSpoolingDelegate.getWriteToken(0));
                    for (int count = 0; count < (tokenAcknowledged - tokenRemoved); count++) {
                        SerializedPage serializedPage = serializedPartitionedPages.get(0).poll();
                        bufferSize.getAndAdd(-serializedPage.getSizeInBytes());
                    }
                    tokenRemoved = tokenAcknowledged;
                }
                if (noMorePages) {
                    if (serializedPartitionedPages.size() != 0) {
                        long tokenAcknowledged = hybridSpoolingDelegate.getWriteToken(0);
                        if (serializedPartitionedPages.containsKey(0) && serializedPartitionedPages.get(0).size() == 0) {
                            serializedPartitionedPages.remove(0);
                        }
                        for (int count = 0; count < (tokenAcknowledged - tokenRemoved) && serializedPartitionedPages.containsKey(bufferId.getId()); count++) {
                            SerializedPage serializedPage = serializedPartitionedPages.get(0).poll();
                            bufferSize.getAndAdd(-serializedPage.getSizeInBytes());
                            if (serializedPartitionedPages.get(0).size() == 0) {
                                serializedPartitionedPages.remove(0);
                            }
                        }
                        tokenRemoved = tokenAcknowledged;
                    }
                    else {
                        hybridSpoolingDelegate.setNoMorePages();
                        outputBuffer.setNoMorePages();
                    }
                }
            }
            if (outputBuffer instanceof PartitionedOutputBuffer) {
                if (!clientAcknowledged) {
                    long tokenAcknowledged = Math.min(outputBuffer.getTokenId(bufferId.getId(), token), hybridSpoolingDelegate.getWriteToken(bufferId.getId()));
                    for (int count = 0; partitionedTokenIds.containsKey(bufferId.getId()) && count < (tokenAcknowledged - partitionedTokenIds.get(bufferId.getId())); count++) {
                        SerializedPage serializedPage = serializedPartitionedPages.get(bufferId.getId()).poll();
                        bufferSize.getAndAdd(-serializedPage.getSizeInBytes());
                    }
                    partitionedTokenIds.put(bufferId.getId(), tokenAcknowledged);
                }
                if (noMorePages) {
                    if (serializedPartitionedPages .size() != 0) {
                        long tokenAcknowledged = hybridSpoolingDelegate.getWriteToken(bufferId.getId());
                        if (serializedPartitionedPages.get(bufferId.getId()).size() == 0) {
                            serializedPartitionedPages.remove(bufferId.getId());
                        }
                        for (int count = 0; partitionedTokenIds.containsKey(bufferId.getId()) && count < (tokenAcknowledged - partitionedTokenIds.get(bufferId.getId())); count++) {
                            SerializedPage serializedPage = serializedPartitionedPages.get(bufferId.getId()).poll();
                            bufferSize.getAndAdd(-serializedPage.getSizeInBytes());
                            if (serializedPartitionedPages.get(bufferId.getId()).size() == 0) {
                                serializedPartitionedPages.remove(bufferId.getId());
                            }
                        }
                        partitionedTokenIds.put(bufferId.getId(), tokenAcknowledged);
                    }
                }
                else {
                    hybridSpoolingDelegate.setNoMorePages();
                    outputBuffer.setNoMorePages();
                }
            }
        }
        outputBuffer.acknowledge(bufferId, token);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                abortedBuffers.add(bufferId);
                // Normally, we should free any pending readers for this buffer,
                // but we assume that the real buffer will be created quickly.
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.abort(bufferId);
    }

    @Override
    public void abort()
    {
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                if (delegate == null) {
                    stateMachine.abort();
                    return;
                }
                outputBuffer = delegate;
            }
        }
        outputBuffer.abort();
    }

    @Override
    public Optional<Throwable> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.isFull();
    }

    @Override
    public void enqueue(List<SerializedPage> pages, String origin)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        if (hybridSpoolingDelegate != null && isTaskAsyncParallelWriteEnabled && delegate != null && !(delegate instanceof  BroadcastOutputBuffer)) {
            if (!serializedPartitionedPages.containsKey(0)) {
                serializedPartitionedPages.put(0, new ConcurrentLinkedQueue<>());
            }
            serializedPartitionedPages.get(0).addAll(pages);
            pages.stream().forEach(serializedPage -> bufferSize.addAndGet(serializedPage.getSizeInBytes()));
            hybridSpoolingDelegate.enqueue(pages, origin);
        }
        outputBuffer.enqueue(pages, origin);
    }

    @Override
    public void enqueue(int partition, List<SerializedPage> pages, String origin)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        if (hybridSpoolingDelegate != null && isTaskAsyncParallelWriteEnabled && delegate != null && !(delegate instanceof  BroadcastOutputBuffer)) {
            if (!serializedPartitionedPages.containsKey(partition)) {
                serializedPartitionedPages.put(partition, new ConcurrentLinkedQueue<>());
            }
            serializedPartitionedPages.get(partition).addAll(pages);
            pages.stream().forEach(serializedPage -> bufferSize.addAndGet(serializedPage.getSizeInBytes()));
            hybridSpoolingDelegate.enqueue(partition, pages, origin);
        }
        outputBuffer.enqueue(partition, pages, origin);
    }

    @Override
    public void setNoMorePages()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        if (hybridSpoolingDelegate != null && isTaskAsyncParallelWriteEnabled && delegate != null && !(delegate instanceof  BroadcastOutputBuffer)) {
            noMorePages = true;
        }
        else {
            outputBuffer.setNoMorePages();
        }
    }

    @Override
    public void setSession(Session session)
    {
        isTaskAsyncParallelWriteEnabled = (getRetryPolicy(session) == RetryPolicy.TASK_ASYNC && isTaskAsyncParallelWriteEnabled(session));
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public void destroy()
    {
        OutputBuffer outputBuffer;
        List<PendingRead> bufferPendingReads = ImmutableList.of();
        synchronized (this) {
            if (delegate == null) {
                // ignore destroy if the buffer already in a terminal state.
                if (!stateMachine.setIf(FINISHED, state -> !state.isTerminal())) {
                    return;
                }

                bufferPendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
        }

        // if there is no output buffer, free the pending reads
        if (outputBuffer == null) {
            for (PendingRead pendingRead : bufferPendingReads) {
                pendingRead.getFutureResult().set(emptyResults(0, true));
            }
            return;
        }

        outputBuffer.destroy();
    }

    @Override
    public void fail()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore fail if the buffer already in a terminal state.
                stateMachine.setIf(FAILED, state -> !state.isTerminal());

                // Do not free readers on fail
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.fail();
    }

    @Override
    public long getPeakMemoryUsage()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        if (outputBuffer != null) {
            return outputBuffer.getPeakMemoryUsage();
        }
        return 0;
    }

    private static class PendingRead
    {
        private final OutputBufferId bufferId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        private final ExtendedSettableFuture<BufferResult> futureResult = ExtendedSettableFuture.create();

        public PendingRead(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.startingSequenceId = startingSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public ExtendedSettableFuture<BufferResult> getFutureResult()
        {
            return futureResult;
        }

        public void process(OutputBuffer delegate)
        {
            if (futureResult.isDone()) {
                return;
            }

            try {
                ListenableFuture<BufferResult> result = delegate.get(bufferId, startingSequenceId, maxSize);
                futureResult.setAsync(result);
            }
            catch (Exception e) {
                futureResult.setException(e);
            }
        }
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.setTaskContext(taskContext);
    }

    @Override
    public void setNoMoreInputChannels()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.setNoMoreInputChannels();
    }

    @Override
    public void addInputChannel(String inputId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.addInputChannel(inputId);
    }

    @Override
    public boolean isSpoolingOutputBuffer()
    {
        return delegate != null && delegate.isSpoolingOutputBuffer();
    }

    public DirectSerialisationType getExchangeDirectSerialisationType()
    {
        DirectSerialisationType type = DirectSerialisationType.OFF;
        if (delegate != null) {
            type = delegate.getExchangeDirectSerialisationType();
        }
        return type;
    }

    @Override
    public void enqueuePages(int partition, List<Page> pages, String id, PagesSerde directSerde)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.enqueuePages(partition, pages, id, directSerde);
    }

    @Override
    public boolean isSpoolingDelegateAvailable()
    {
        return delegate != null && hybridSpoolingDelegate != null && hybridSpoolingDelegate.isSpoolingOutputBuffer();
    }

    @Override
    public OutputBuffer getSpoolingDelegate()
    {
        return hybridSpoolingDelegate;
    }

    @Override
    public OutputBuffer getDelegate()
    {
        return delegate;
    }

    @Override
    public DirectSerialisationType getDelegateSpoolingExchangeDirectSerializationType()
    {
        DirectSerialisationType type = DirectSerialisationType.JAVA;
        if (hybridSpoolingDelegate != null) {
            type = hybridSpoolingDelegate.getExchangeDirectSerialisationType();
        }
        return type;
    }

    @Override
    public void setSerde(PagesSerde serde)
    {
        this.serde = serde;
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
}
