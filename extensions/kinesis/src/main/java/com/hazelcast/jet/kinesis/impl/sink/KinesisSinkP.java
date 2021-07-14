/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl.sink;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.kinesis.KinesisSinks;
import com.hazelcast.jet.kinesis.impl.KinesisUtil;
import com.hazelcast.jet.kinesis.impl.RetryTracker;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisSinkP<T> implements Processor {

    /**
     * Each shard can ingest a maximum of a 1000 records per second.
     */
    private static final int MAX_RECORD_PER_SHARD_PER_SECOND = 1000;

    /**
     * PutRecords requests are limited to 500 records.
     */
    private static final int MAX_RECORDS_IN_REQUEST = 500;

    /**
     * Since we are using PutRecords for its batching effect, we don't want
     * the batch size to be so small as to negate all benefits.
     */
    private static final int MIN_RECORDS_IN_REQUEST = 10;

    /**
     * The maximum allowed size of all the records in a PutRecords request,
     * including partition keys is 5M.
     */
    private static final int MAX_REQUEST_SIZE_IN_BYTES = 5 * 1024 * 1024;

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nonnull
    private final ShardCountMonitor monitor;
    @Nonnull
    private final Buffer<T> buffer;

    @Probe(name = KinesisSinks.BATCH_SIZE_METRIC, unit = ProbeUnit.COUNT)
    private final Counter batchSizeMetric;
    @Probe(name = KinesisSinks.THROTTLING_SLEEP_METRIC, unit = ProbeUnit.MS)
    private final Counter sleepMetric = SwCounter.newSwCounter();

    private ILogger logger;
    private int shardCount;
    private int sinkCount;

    private Future<PutRecordsResult> sendResult;
    private long nextSendTime = nanoTime();
    private final RetryTracker sendRetryTracker;

    private final ThroughputController throughputController = new ThroughputController();

    public KinesisSinkP(
            @Nonnull AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn,
            @Nonnull ShardCountMonitor monitor,
            @Nonnull RetryStrategy retryStrategy
            ) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.monitor = monitor;
        this.buffer = new Buffer<>(keyFn, valueFn);
        this.batchSizeMetric = SwCounter.newSwCounter(buffer.getCapacity());
        this.sendRetryTracker = new RetryTracker(retryStrategy);
    }

    @Override
    public boolean isCooperative() {
        return true;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        sinkCount = context.totalParallelism();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true; //watermark ignored
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        monitor.run();

        updateThroughputLimitations();

        if (sendResult != null) {
            checkIfSendingFinished();
        }
        if (sendResult == null) {
            initSending(inbox);
        }
    }

    @Override
    public boolean complete() {
        if (sendResult != null) {
            checkIfSendingFinished();
        }
        if (sendResult == null) {
            if (buffer.isEmpty()) {
                return true;
            }
            initSending(null);
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (sendResult != null) {
            checkIfSendingFinished();
        }
        return sendResult == null;
    }

    private void updateThroughputLimitations() {
        int newShardCount = monitor.shardCount();
        if (newShardCount > 0 && shardCount != newShardCount) {
            buffer.setCapacity(throughputController.computeBatchSize(newShardCount, sinkCount));
            batchSizeMetric.set(buffer.getCapacity());

            shardCount = newShardCount;
        }
    }

    private void initSending(@Nullable Inbox inbox) {
        if (inbox != null) {
            bufferFromInbox(inbox);
        }
        attemptToDispatchBufferContent();
    }

    private void bufferFromInbox(@Nonnull Inbox inbox) {
        for (T t; (t = (T) inbox.peek()) != null && buffer.add(t); ) {
            inbox.remove();
        }
    }

    private void attemptToDispatchBufferContent() {
        if (buffer.isEmpty()) {
            return;
        }

        long currentTime = nanoTime();
        if (currentTime < nextSendTime) {
            return;
        }

        List<PutRecordsRequestEntry> entries = buffer.content();
        sendResult = putRecordsAsync(entries);
        nextSendTime = currentTime;
    }

    private Future<PutRecordsResult> putRecordsAsync(Collection<PutRecordsRequestEntry> entries) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setRecords(entries);
        request.setStreamName(stream);
        return kinesis.putRecordsAsync(request);
    }

    private void checkIfSendingFinished() {
        if (sendResult.isDone()) {
            PutRecordsResult result;
            try {
                result = KinesisUtil.readResult(this.sendResult);
            } catch (ProvisionedThroughputExceededException pte) {
                dealWithThroughputExceeded("Data throughput rate exceeded. Backing off and retrying in %d ms");
                return;
            } catch (SdkClientException sce) {
                dealWithSendFailure(sce);
                return;
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                sendResult = null;
            }

            pruneSentFromBuffer(result);
            if (result.getFailedRecordCount() > 0) {
                dealWithThroughputExceeded("Failed to send " + result.getFailedRecordCount() + " (out of " +
                        result.getRecords().size() + ") record(s) to stream '" + stream +
                        "'. Sending will be retried in %d ms, message reordering is likely.");
            } else {
                long sleepTimeNanos = throughputController.markSuccess();
                this.nextSendTime += sleepTimeNanos;
                this.sleepMetric.set(NANOSECONDS.toMillis(sleepTimeNanos));
                sendRetryTracker.reset();
            }
        }
    }

    private void dealWithSendFailure(@Nonnull Exception failure) {
        sendRetryTracker.attemptFailed();
        if (sendRetryTracker.shouldTryAgain()) {
            long timeoutMillis = sendRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed to send records, will retry in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextSendTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
        } else {
            throw rethrow(failure);
        }

    }

    private void dealWithThroughputExceeded(@Nonnull String message) {
        long sleepTimeNanos = throughputController.markFailure();
        this.nextSendTime += sleepTimeNanos;
        this.sleepMetric.set(NANOSECONDS.toMillis(sleepTimeNanos));
        logger.warning(String.format(message, NANOSECONDS.toMillis(sleepTimeNanos)));
    }

    private void pruneSentFromBuffer(@Nullable PutRecordsResult result) {
        if (result == null) {
            return;
        }

        List<PutRecordsResultEntry> resultEntries = result.getRecords();
        if (result.getFailedRecordCount() > 0) {
            buffer.retainFailedEntries(resultEntries);
        } else {
            buffer.clear();
        }
    }

    private static class Buffer<T> {

        private final FunctionEx<T, String> keyFn;
        private final FunctionEx<T, byte[]> valueFn;

        private final BufferEntry[] entries;
        private int entryCount;
        private int totalEntrySize;
        private int capacity;

        Buffer(FunctionEx<T, String> keyFn, FunctionEx<T, byte[]> valueFn) {
            this.keyFn = keyFn;
            this.valueFn = valueFn;
            this.entries = initEntries();
            this.capacity = entries.length;
        }

        public int getCapacity() {
            return capacity;
        }

        void setCapacity(int capacity) {
            if (capacity < 0 || capacity > entries.length) {
                throw new IllegalArgumentException("Capacity limited to [0, " + entries.length + ")");
            }
            this.capacity = capacity;
        }

        boolean add(T item) {
            if (isFull()) {
                return false;
            }

            String key = keyFn.apply(item);
            if (key.isEmpty()) {
                throw new JetException("Key empty");
            }
            int unicodeCharsInKey = key.length();
            if (unicodeCharsInKey > KinesisSinks.MAXIMUM_KEY_LENGTH) {
                throw new JetException("Key too long");
            }
            int keyLength = getKeyLength(key);

            byte[] value = valueFn.apply(item);
            int itemLength = value.length + keyLength;
            if (itemLength > KinesisSinks.MAX_RECORD_SIZE) {
                throw new JetException("Encoded length (key + payload) is too big");
            }

            if (totalEntrySize + itemLength > MAX_REQUEST_SIZE_IN_BYTES) {
                return false;
            } else {
                totalEntrySize += itemLength;

                BufferEntry entry = entries[entryCount++];
                entry.set(key, value, itemLength);

                return true;
            }
        }

        public void retainFailedEntries(List<PutRecordsResultEntry> results) {
            assert results.size() == entryCount;

            int startIndex = 0;
            for (int index = 0; index < results.size(); index++) {
                if (results.get(index).getErrorCode() != null) {
                    swap(startIndex++, index);
                } else {
                    totalEntrySize -= entries[index].encodedSize;
                    entryCount--;
                }
            }
        }

        private void swap(int a, int b) {
            BufferEntry temp = entries[a];
            entries[a] = entries[b];
            entries[b] = temp;
        }

        void clear() {
            entryCount = 0;
            totalEntrySize = 0;
        }

        boolean isEmpty() {
            return entryCount == 0;
        }

        public boolean isFull() {
            return entryCount == entries.length || entryCount >= capacity;
        }

        public List<PutRecordsRequestEntry> content() {
            return Arrays.stream(entries)
                    .limit(entryCount)
                    .map(e -> e.putRecordsRequestEntry)
                    .collect(Collectors.toList());
        }

        private int getKeyLength(String key) {
            // just an estimation; for exact length we would need to figure out
            // how many bytes UTF-8 encoding would produce; estimation is good
            // enough, Kinesis will reject what we miss anyways
            return key.length();
        }

        private static BufferEntry[] initEntries() {
            return IntStream.range(0, MAX_RECORDS_IN_REQUEST).boxed()
                    .map(IGNORED -> new BufferEntry())
                    .toArray(BufferEntry[]::new);
        }
    }

    private static final class BufferEntry {

        private PutRecordsRequestEntry putRecordsRequestEntry;
        private int encodedSize;

        public void set(String partitionKey, byte[] data, int size) {
            if (putRecordsRequestEntry == null) {
                putRecordsRequestEntry = new PutRecordsRequestEntry();
            }

            putRecordsRequestEntry.setPartitionKey(partitionKey);

            ByteBuffer byteBuffer = putRecordsRequestEntry.getData();
            if (byteBuffer == null || byteBuffer.capacity() < data.length) {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(data));
            } else {
                ((java.nio.Buffer) byteBuffer).clear(); //cast needed due to JDK 9 breaking compatibility
                byteBuffer.put(data);
                ((java.nio.Buffer) byteBuffer).flip(); //cast needed due to JDK 9 breaking compatibility
            }

            encodedSize = size;
        }
    }

    /**
     * Under normal circumstances, when our sinks don't saturate the stream
     * (ie. they don't send out more data than the sink can ingest), sinks
     * will not sleep or introduce any kind of delay between two consecutive
     * send operations (as long as there is data to send).
     * <p>
     * When the stream's ingestion rate is reached however, we want the sinks to
     * slow down the sending process. They do it by adjusting the send batch
     * size on one hand, and by introducing a sleep after each send operation.
     * <p>
     * The sleep duration is continuously adjusted to find the best value. The
     * ideal situation we try to achieve is that we never trip the stream's
     * ingestion rate limiter, while also sending out data with the maximum
     * rate possible.
     */
    private static final class ThroughputController extends SleepController {

        /**
         * The ideal sleep duratio after sends, while rate limiting is necessary.
         */
        private static final int IDEAL_SLEEP_MS = 250;

        int computeBatchSize(int shardCount, int sinkCount) {
            if (shardCount < 1) {
                throw new IllegalArgumentException("Invalid shard count: " + shardCount);
            }
            if (sinkCount < 1) {
                throw new IllegalArgumentException("Invalid sink count: " + sinkCount);
            }

            int totalRecordsPerSecond = MAX_RECORD_PER_SHARD_PER_SECOND * shardCount;
            int recordPerSinkPerSecond = totalRecordsPerSecond / sinkCount;
            int computedBatchSize = recordPerSinkPerSecond / (int) (SECONDS.toMillis(1) / IDEAL_SLEEP_MS);

            if (computedBatchSize > MAX_RECORDS_IN_REQUEST) {
                return MAX_RECORDS_IN_REQUEST;
            } else {
                return Math.max(computedBatchSize, MIN_RECORDS_IN_REQUEST);
            }
        }

    }
}
