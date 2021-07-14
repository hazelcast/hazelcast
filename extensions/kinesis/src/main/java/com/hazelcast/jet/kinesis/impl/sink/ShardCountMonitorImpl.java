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
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import com.hazelcast.jet.kinesis.impl.AbstractShardWorker;
import com.hazelcast.jet.kinesis.impl.KinesisUtil;
import com.hazelcast.jet.kinesis.impl.RandomizedRateTracker;
import com.hazelcast.jet.kinesis.impl.RetryTracker;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

final class ShardCountMonitorImpl extends AbstractShardWorker implements ShardCountMonitor {

    /**
     * DescribeStreamSummary operations are limited to 20 per second, per account.
     */
    private static final int DESCRIBE_STREAM_OPERATIONS_ALLOWED_PER_SECOND = 20;

    /**
     * We don't want to issue describe stream operations at the peak allowed rate.
     */
    private static final double RATIO_OF_DESCRIBE_STREAM_RATE_UTILIZED = 0.1;

    private final AtomicInteger shardCount = new AtomicInteger();
    private final RandomizedRateTracker describeStreamRateTracker;
    private final RetryTracker describeStreamRetryTracker;

    private Future<DescribeStreamSummaryResult> describeStreamResult;
    private long nextDescribeStreamTime;

    ShardCountMonitorImpl(
            int knownShardCounterInstances,
            AmazonKinesisAsync kinesis,
            String streamName,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(kinesis, streamName, logger);
        this.describeStreamRetryTracker = new RetryTracker(retryStrategy);
        this.describeStreamRateTracker = initRandomizedTracker(knownShardCounterInstances);
        this.nextDescribeStreamTime = System.nanoTime();
    }

    @Override
    public void run() {
        if (describeStreamResult == null) {
            initDescribeStream();
        } else {
            checkForStreamDescription();
        }
    }

    public AtomicInteger getSharedShardCounter() {
        return shardCount;
    }

    @Override
    public int shardCount() {
        return shardCount.get();
    }

    private void initDescribeStream() {
        long currentTime = System.nanoTime();
        if (currentTime < nextDescribeStreamTime) {
            return;
        }
        describeStreamResult = describeStreamSummaryAsync();
        nextDescribeStreamTime = currentTime + describeStreamRateTracker.next();
    }

    private Future<DescribeStreamSummaryResult> describeStreamSummaryAsync() {
        DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
        request.setStreamName(streamName);
        return kinesis.describeStreamSummaryAsync(request);
    }

    private void checkForStreamDescription() {
        if (!describeStreamResult.isDone()) {
            return;
        }

        DescribeStreamSummaryResult result;
        try {
            result = KinesisUtil.readResult(describeStreamResult);
        } catch (SdkClientException e) {
            dealWithDescribeStreamFailure(e);
            return;
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            describeStreamResult = null;
        }
        describeStreamRetryTracker.reset();

        StreamDescriptionSummary streamDescription = result.getStreamDescriptionSummary();
        if (streamDescription == null) {
            return;
        }

        Integer newShardCount = streamDescription.getOpenShardCount();
        if (newShardCount == null) {
            return;
        }

        int oldShardCount = shardCount.getAndSet(newShardCount);
        if (oldShardCount != newShardCount) {
            logger.info(String.format("Updated shard count for stream '%s': %d", streamName, newShardCount));
        }
    }

    private void dealWithDescribeStreamFailure(@Nonnull Exception failure) {
        describeStreamRetryTracker.attemptFailed();
        if (describeStreamRetryTracker.shouldTryAgain()) {
            long timeoutMillis = describeStreamRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed obtaining stream description, retrying in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextDescribeStreamTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
        } else {
            throw rethrow(failure);
        }
    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int knownShardCountInstances) {
        // The maximum rate at which DescribeStreamSummary operations can be
        // performed on a data stream is 20/second and we need to enforce this,
        // even while we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toNanos(1) * knownShardCountInstances,
                (int) (DESCRIBE_STREAM_OPERATIONS_ALLOWED_PER_SECOND * RATIO_OF_DESCRIBE_STREAM_RATE_UTILIZED));
    }
}

/**
 * An implementation that doesn't query the shard count, but simply returns
 * the count from the given counter.
 */
final class NoopShardCountMonitor implements ShardCountMonitor {

    private final AtomicInteger shardCount;

    NoopShardCountMonitor(AtomicInteger shardCount) {
        this.shardCount = shardCount;
    }

    @Override
    public void run() {
    }

    @Override
    public int shardCount() {
        return shardCount.get();
    }
}
