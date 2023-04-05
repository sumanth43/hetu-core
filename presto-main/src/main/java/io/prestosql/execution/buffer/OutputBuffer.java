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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.Session;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public interface OutputBuffer
{
    /**
     * Set task context
     *
     * @param taskContext task context
     */
    void setTaskContext(@NotNull TaskContext taskContext);

    /**
     * Indicate no more input channels will be added to this output buffer
     */
    void setNoMoreInputChannels();

    /**
     * Add an input channel identifier
     *
     * @param inputId input channel identifier
     */
    void addInputChannel(@NotNull String inputId);

    /**
     * Gets the current state of this buffer.  This method is guaranteed to not block or acquire
     * contended locks, but the stats in the info object may be internally inconsistent.
     */
    OutputBufferInfo getInfo();

    /**
     * A buffer is finished once no-more-pages has been set and all buffers have been closed
     * with an abort call.
     */
    boolean isFinished();

    /**
     * Get the memory utilization percentage.
     */
    double getUtilization();

    /**
     * Check if the buffer is blocking producers.
     */
    boolean isOverutilized();

    /**
     * Add a listener which fires anytime the buffer state changes.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener);

    /**
     * Updates the buffer configuration.
     */
    void setOutputBuffers(OutputBuffers newOutputBuffers);

    /**
     * Gets pages from the output buffer, and acknowledges all pages received from the last
     * request.  The initial token is zero. Subsequent tokens are acquired from the
     * next token field in the BufferResult returned from the previous request.
     * If the buffer result is marked as complete, the client must call abort to acknowledge
     * receipt of the final state.
     */
    ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize);

    /**
     * Acknowledges the previously received pages from the output buffer.
     */
    void acknowledge(OutputBufferId bufferId, long token);

    /**
     * Closes the specified output buffer.
     */
    void abort(OutputBufferId bufferId);

    /**
     * Abort the buffer, discarding all pages, but blocking readers. Its is expected
     * readers will be unblocked when the failed query is cleaned up.
     */
    void abort();

    /**
     *
     * @return non-empty failure cause if the buffer is in state {@link BufferState#FAILED}
     */
    Optional<Throwable> getFailureCause();

    /**
     * Get a future that will be completed when the buffer is not full.
     */
    ListenableFuture<?> isFull();

    /**
     * Adds a split-up page to an unpartitioned buffer. If no-more-pages has been set, the enqueue
     * page call is ignored.  This can happen with limit queries.
     */
    void enqueue(List<SerializedPage> pages, String origin);

    /**
     * Adds a split-up page to a specific partition.  If no-more-pages has been set, the enqueue
     * page call is ignored.  This can happen with limit queries.
     */
    void enqueue(int partition, List<SerializedPage> pages, String origin);

    /**
     * Notify buffer that no more pages will be added. Any future calls to enqueue a
     * page are ignored.
     */
    void setNoMorePages();

    /**
     * Get buffer state
     */
    BufferState getState();

    /**
     * Destroys the buffer, discarding all pages.
     */
    void destroy();

    /**
     * Fail the buffer, discarding all pages, but blocking readers.  It is expected that
     * readers will be unblocked when the failed query is cleaned up.
     */
    void fail();

    /**
     * @return the peak memory usage of this output buffer.
     */
    long getPeakMemoryUsage();

    /**
     * @return true in case of spooling output buffer.
     */
    default boolean isSpoolingOutputBuffer()
    {
        return false;
    }

    default DirectSerialisationType getExchangeDirectSerialisationType()
    {
        return DirectSerialisationType.OFF;
    }

    default void enqueuePages(int partition, List<Page> pages, String id, PagesSerde directSerde)
    {
        return;
    }

    default boolean isSpoolingDelegateAvailable()
    {
        return false;
    }

    default OutputBuffer getSpoolingDelegate()
    {
        return null;
    }

    default OutputBuffer getDelegate()
    {
        return null;
    }

    default DirectSerialisationType getDelegateSpoolingExchangeDirectSerializationType()
    {
        return DirectSerialisationType.JAVA;
    }

    default void setSerde(PagesSerde pagesSerde)
    {
    }

    default void setJavaSerde(PagesSerde pagesSerde)
    {
    }

    default void setKryoSerde(PagesSerde pagesSerde)
    {
    }

    default boolean checkIfAcknowledged(int bufferId, long token)
    {
        return true;
    }

    default long getTokenId(int bufferId, long token)
    {
        return -1;
    }

    default long getWriteToken(int partition)
    {
        return -1;
    }

    default void setSession(Session session)
    {}
}
