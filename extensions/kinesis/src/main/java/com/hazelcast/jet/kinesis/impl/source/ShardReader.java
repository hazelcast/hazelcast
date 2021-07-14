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

package com.hazelcast.jet.kinesis.impl.source;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.kinesis.KinesisSources;
import com.hazelcast.jet.kinesis.impl.AbstractShardWorker;
import com.hazelcast.jet.kinesis.impl.KinesisUtil;
import com.hazelcast.jet.kinesis.impl.RandomizedRateTracker;
import com.hazelcast.jet.kinesis.impl.RetryTracker;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ShardReader extends AbstractShardWorker implements DynamicMetricsProvider {

    /* Kinesis allows for a maximum of 5 GetRecords operations per second. */
    private static final int GET_RECORD_OPS_PER_SECOND = 5;

    /**
     * Amount of time to sleep before retrying the GetRecords operation,
     * if throughput is exceeded. Should never happen, because we have
     * throughput limitation in place.
     */
    private static final int SLEEP_ON_THROUGHPUT_EXCEEDED_MS = 200;

    private final Shard shard;
    private final RandomizedRateTracker getRecordsRateTracker =
            new RandomizedRateTracker(1000, GET_RECORD_OPS_PER_SECOND);

    @Probe(name = KinesisSources.MILLIS_BEHIND_LATEST_METRIC)
    private final Counter millisBehindLatestMetric = SwCounter.newSwCounter(-1);

    private final RetryTracker getShardIteratorRetryTracker;
    private final RetryTracker readRecordRetryTracker;
    private final InitialShardIterators initialShardIterators;

    private State state = State.NO_SHARD_ITERATOR;
    private String shardIterator;

    private Future<GetShardIteratorResult> shardIteratorResult;
    private long nextGetShardIteratorTime = System.nanoTime();

    private Future<GetRecordsResult> recordsResult;
    private long nextGetRecordsTime = System.nanoTime();

    private List<Record> data = Collections.emptyList();
    private String lastSeenSeqNo;

    ShardReader(
            AmazonKinesisAsync kinesis,
            String stream,
            Shard shard,
            String lastSeenSeqNo,
            RetryStrategy retryStrategy,
            InitialShardIterators initialShardIterators,
            ILogger logger
    ) {
        super(kinesis, stream, logger);
        this.shard = shard;
        this.lastSeenSeqNo = lastSeenSeqNo;
        this.readRecordRetryTracker = new RetryTracker(retryStrategy);
        this.getShardIteratorRetryTracker = new RetryTracker(retryStrategy);
        this.initialShardIterators = initialShardIterators;
    }

    public Result probe(long currentTime) {
        switch (state) {
            case NO_SHARD_ITERATOR:
                return handleNoShardIterator(currentTime);
            case WAITING_FOR_SHARD_ITERATOR:
                return handleWaitingForShardIterator();
            case NEED_TO_REQUEST_RECORDS:
                return handleNeedToRequestRecords(currentTime);
            case WAITING_FOR_RECORDS:
                return handleWaitingForRecords();
            case HAS_DATA_NEED_TO_REQUEST_RECORDS:
                return handleHasDataNeedToRequestRecords(currentTime);
            case HAS_DATA:
                return handleHasData();
            case SHARD_CLOSED:
                return handleShardClosed();
            default:
                throw new JetException("Programming error, unhandled state: " + state);
        }
    }

    private Result handleNoShardIterator(long currentTime) {
        if (attemptToSendGetShardIteratorRequest(currentTime)) {
            state = State.WAITING_FOR_SHARD_ITERATOR;
        }
        return Result.NOTHING;
    }

    private boolean attemptToSendGetShardIteratorRequest(long currentTime) {
        if (currentTime < nextGetShardIteratorTime) {
            return false;
        }
        GetShardIteratorRequest request = getShardIteratorRequest();
        shardIteratorResult = kinesis.getShardIteratorAsync(request);
        nextGetShardIteratorTime = currentTime;
        return true;
    }

    @Nonnull
    private GetShardIteratorRequest getShardIteratorRequest() {
        if (lastSeenSeqNo == null) {
            return initialShardIterators.request(streamName, shard);
        } else {
            GetShardIteratorRequest request = new GetShardIteratorRequest();
            request.setStreamName(streamName);
            request.setShardId(shard.getShardId());
            request.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            request.setStartingSequenceNumber(lastSeenSeqNo);
            return request;
        }
    }

    private Result handleWaitingForShardIterator() {
        if (shardIteratorResult.isDone()) {
            try {
                shardIterator = KinesisUtil.readResult(shardIteratorResult).getShardIterator();
            } catch (SdkClientException sce) {
                return dealWithGetShardIteratorFailure(sce);
            } catch (Throwable t) {
                throw rethrow(t);
            }

            getShardIteratorRetryTracker.reset();

            state = State.NEED_TO_REQUEST_RECORDS;
        }
        return Result.NOTHING;
    }

    @Nonnull
    private Result dealWithGetShardIteratorFailure(Exception e) {
        getShardIteratorRetryTracker.attemptFailed();
        if (getShardIteratorRetryTracker.shouldTryAgain()) {
            long timeoutMillis = getShardIteratorRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed retrieving shard iterator, retrying in %d ms.Cause: %s", timeoutMillis,
                    e.getMessage()));
            nextGetShardIteratorTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
            state = State.NO_SHARD_ITERATOR;
            return Result.NOTHING;
        } else {
            throw rethrow(e);
        }
    }

    private Result handleNeedToRequestRecords(long currentTime) {
        if (attemptToSendGetRecordsRequest(currentTime)) {
            state = State.WAITING_FOR_RECORDS;
        }

        return Result.NOTHING;
    }

    private Result handleWaitingForRecords() {
        if (recordsResult.isDone()) {
            GetRecordsResult result;
            try {
                result = KinesisUtil.readResult(recordsResult);
            } catch (ProvisionedThroughputExceededException pte) {
                return dealWithThroughputExceeded();
            } catch (SdkClientException sce) {
                return dealWithReadRecordFailure(sce);
            } catch (Throwable t) {
                throw rethrow(t);
            }

            readRecordRetryTracker.reset();

            shardIterator = result.getNextShardIterator();
            data = result.getRecords();
            if (!data.isEmpty()) {
                lastSeenSeqNo = data.get(data.size() - 1).getSequenceNumber();
                millisBehindLatestMetric.set(result.getMillisBehindLatest());
            }
            if (shardIterator == null) {
                state = State.SHARD_CLOSED;
                return data.isEmpty() ? Result.CLOSED : Result.HAS_DATA;
            } else if (!data.isEmpty()) {
                state = State.HAS_DATA_NEED_TO_REQUEST_RECORDS;
                return Result.HAS_DATA;
            } else {
                state = State.NEED_TO_REQUEST_RECORDS;
                return Result.NOTHING;
            }
        } else {
            return Result.NOTHING;
        }
    }

    private Result dealWithThroughputExceeded() {
        long timeoutMillis = SLEEP_ON_THROUGHPUT_EXCEEDED_MS;
        logger.warning(String.format("Data throughput rate exceeded. Backing off and retrying in %d ms.", timeoutMillis));
        updateGetNextRecordsTime(System.nanoTime(), MILLISECONDS.toNanos(timeoutMillis));
        state = State.NEED_TO_REQUEST_RECORDS;
        return Result.NOTHING;
    }

    private Result dealWithReadRecordFailure(Exception e) {
        readRecordRetryTracker.attemptFailed();
        if (readRecordRetryTracker.shouldTryAgain()) {
            long timeoutMillis = readRecordRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed reading records, retrying in %d. Cause: %s",
                    timeoutMillis, e.getMessage()));
            updateGetNextRecordsTime(System.nanoTime(), MILLISECONDS.toNanos(timeoutMillis));
            state = State.NEED_TO_REQUEST_RECORDS;
            return Result.NOTHING;
        } else {
            throw rethrow(e);
        }
    }

    private Result handleHasDataNeedToRequestRecords(long currentTime) {
        if (attemptToSendGetRecordsRequest(currentTime)) {
            state = data.isEmpty() ? State.WAITING_FOR_RECORDS : State.HAS_DATA;
        }

        return data.isEmpty() ? Result.NOTHING : Result.HAS_DATA;
    }

    private Result handleHasData() {
        state = data.isEmpty() ? State.WAITING_FOR_RECORDS : State.HAS_DATA;
        return data.isEmpty() ? Result.NOTHING : Result.HAS_DATA;
    }

    private Result handleShardClosed() {
        return data.isEmpty() ? Result.CLOSED : Result.HAS_DATA;
    }

    private boolean attemptToSendGetRecordsRequest(long currentTime) {
        if (currentTime < nextGetRecordsTime) {
            return false;
        }
        recordsResult = getRecordsAsync(shardIterator);
        updateGetNextRecordsTime(currentTime, 0L);
        return true;
    }

    private Future<GetRecordsResult> getRecordsAsync(String shardIterator) {
        GetRecordsRequest request = new GetRecordsRequest();
        request.setShardIterator(shardIterator);
        return kinesis.getRecordsAsync(request);
    }

    public Shard getShard() {
        return shard;
    }

    public String getLastSeenSeqNo() {
        return lastSeenSeqNo;
    }

    public Traverser<Record> clearData() {
        if (data.isEmpty()) {
            throw new IllegalStateException("Can't ask for data when none is available");
        }

        Traverser<Record> traverser = Traversers.traverseIterable(data);
        data = Collections.emptyList();
        return traverser;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        descriptor = descriptor.withTag("shard", shard.getShardId());
        context.collect(descriptor, this);
    }

    private void updateGetNextRecordsTime(long currentTime, long minimumIncrease) {
        nextGetRecordsTime = currentTime + Math.max(minimumIncrease, getRecordsRateTracker.next());
    }

    enum Result {
        /**
         * Running the reader has not produced any events that need handling.
         */
        NOTHING,

        /**
         * Running the reader has produced new data to be processed.
         */
        HAS_DATA,

        /**
         * The shard was read up to its end (due to merge or split).
         */
        CLOSED
    }

    private enum State {
        /**
         * Shard iterator not available and has not been requested.
         */
        NO_SHARD_ITERATOR,

        /**
         * Shard iterator not available but has already been requested.
         */
        WAITING_FOR_SHARD_ITERATOR,

        /**
         * Has no data available, ready to read some.
         */
        NEED_TO_REQUEST_RECORDS,

        /**
         * Has no data, reading records initiated, waiting for results.
         */
        WAITING_FOR_RECORDS,

        /**
         * Has some data read previously, ready to issue a request for more.
         * The previously read data might get processed while more arrives.
         */
        HAS_DATA_NEED_TO_REQUEST_RECORDS,

        /**
         * Has some data read previously, reading more initiated, waiting for
         * the processing of what's already available to finish.
         */
        HAS_DATA,

        /**
         * Shard has been terminated, due to a split or a merge.
         */
        SHARD_CLOSED,
        ;
    }

}
