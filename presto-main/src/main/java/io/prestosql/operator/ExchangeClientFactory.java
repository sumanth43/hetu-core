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
package io.prestosql.operator;

import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import io.prestosql.exchange.ExchangeId;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.TaskFailureListener;
import io.prestosql.failuredetector.FailureDetectorManager;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.QueryId;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ExchangeClientFactory
        implements ExchangeClientSupplier
{
    private final DataSize maxBufferedBytes;
    private final int concurrentRequestMultiplier;
    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final boolean acknowledgePages;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ExecutorService pageBufferClientCallbackExecutor;
    private final FailureDetectorManager failureDetectorManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final DataSize deduplicationBufferSize;

    @Inject
    public ExchangeClientFactory(
            ExchangeClientConfig config,
            @ForExchange HttpClient httpClient,
            @ForExchange ScheduledExecutorService scheduler,
            FailureDetectorManager failureDetectorManager,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        this(
                config.getMaxBufferSize(),
                config.getDeduplicationBufferSize(),
                config.getMaxResponseSize(),
                config.getConcurrentRequestMultiplier(),
                config.isAcknowledgePages(),
                config.getPageBufferClientMaxCallbackThreads(),
                httpClient,
                scheduler,
                failureDetectorManager,
                exchangeManagerRegistry);
    }

    public ExchangeClientFactory(
            DataSize maxBufferedBytes,
            DataSize deduplicationBufferSize,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            boolean acknowledgePages,
            int pageBufferClientMaxCallbackThreads,
            HttpClient httpClient,
            ScheduledExecutorService scheduler,
            FailureDetectorManager failureDetectorManager,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.acknowledgePages = acknowledgePages;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.failureDetectorManager = failureDetectorManager;

        // Use only 0.75 of the maxResponseSize to leave room for additional bytes from the encoding
        // TODO figure out a better way to compute the size of data that will be transferred over the network
        requireNonNull(maxResponseSize, "maxResponseSize is null");
        long maxResponseSizeBytes = (long) (Math.min(httpClient.getMaxContentLength(), maxResponseSize.toBytes()) * 0.75);
        this.maxResponseSize = new DataSize(maxResponseSizeBytes, BYTE);

        this.scheduler = requireNonNull(scheduler, "scheduler is null");

        this.pageBufferClientCallbackExecutor = newFixedThreadPool(pageBufferClientMaxCallbackThreads, daemonThreadsNamed("page-buffer-client-callback-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) pageBufferClientCallbackExecutor);
        this.deduplicationBufferSize = requireNonNull(deduplicationBufferSize, "deduplicationBufferSize is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferSize must be at least 1 byte: %s", maxBufferedBytes);
        checkArgument(maxResponseSize.toBytes() > 0, "maxResponseSize must be at least 1 byte: %s", maxResponseSize);
        checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s", concurrentRequestMultiplier);
    }

    @PreDestroy
    public void stop()
    {
        pageBufferClientCallbackExecutor.shutdownNow();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Override
    public ExchangeClient get(
            LocalMemoryContext systemMemoryContext,
            TaskFailureListener taskFailureListener,
            RetryPolicy retryPolicy,
            ExchangeId exchangeId,
            QueryId queryId)
    {
        DirectExchangeBuffer buffer;
        switch (retryPolicy) {
            case TASK:
                buffer = new DeduplicatingDirectExchangeBuffer(scheduler, deduplicationBufferSize, retryPolicy, exchangeManagerRegistry, queryId, exchangeId);
                break;
            case NONE:
            case TASK_ASYNC:
                buffer = null;
                break;
            default:
                throw new IllegalArgumentException("unexpected retry policy: " + retryPolicy);
        }

        return new ExchangeClient(
                maxBufferedBytes,
                maxResponseSize,
                concurrentRequestMultiplier,
                acknowledgePages,
                httpClient,
                scheduler,
                systemMemoryContext,
                pageBufferClientCallbackExecutor,
                failureDetectorManager,
                taskFailureListener,
                retryPolicy,
                buffer);
    }
}
