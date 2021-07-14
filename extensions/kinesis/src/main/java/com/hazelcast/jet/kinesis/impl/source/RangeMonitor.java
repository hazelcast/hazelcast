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
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.hazelcast.jet.kinesis.impl.AbstractShardWorker;
import com.hazelcast.jet.kinesis.impl.KinesisUtil;
import com.hazelcast.jet.kinesis.impl.RandomizedRateTracker;
import com.hazelcast.jet.kinesis.impl.RetryTracker;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public class RangeMonitor extends AbstractShardWorker {

    /**
     * ListStreams operations are limited to 100 per second, per data stream.
     */
    private static final int SHARD_LISTINGS_ALLOWED_PER_SECOND = 100;

    /**
     * We don't want to issue shard listing requests at the peak allowed rate.
     */
    private static final double RATIO_OF_SHARD_LISTING_RATE_UTILIZED = 0.1;

    private final ShardTracker shardTracker;
    private final HashRange memberHashRange;
    private final ShardQueue[] shardQueues;
    private final RandomizedRateTracker listShardsRateTracker;
    private final RetryTracker listShardRetryTracker;

    private String nextToken;
    private Future<ListShardsResult> listShardsResult;
    private long nextListShardsTimeMs;

    public RangeMonitor(
            int totalInstances,
            AmazonKinesisAsync kinesis,
            String stream,
            HashRange memberHashRange,
            HashRange[] rangePartitions,
            ShardQueue[] shardQueues,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(kinesis, stream, logger);
        this.memberHashRange = memberHashRange;
        this.shardTracker = new ShardTracker(rangePartitions);
        this.shardQueues = shardQueues;
        this.listShardRetryTracker = new RetryTracker(retryStrategy);
        this.listShardsRateTracker = initRandomizedTracker(totalInstances);
        this.nextListShardsTimeMs = System.currentTimeMillis();
    }

    public void run() {
        long currentTimeMs = System.currentTimeMillis();
        if (listShardsResult == null) {
            initShardListing(currentTimeMs);
        } else {
            if (listShardsResult.isDone()) {
                ListShardsResult result;
                try {
                    result = KinesisUtil.readResult(listShardsResult);
                } catch (SdkClientException e) {
                    dealWithListShardsFailure(e);
                    return;
                } catch (Throwable t) {
                    throw rethrow(t);
                } finally {
                    listShardsResult = null;
                }

                listShardRetryTracker.reset();

                checkForNewShards(currentTimeMs, result);

                nextToken = result.getNextToken();
                if (nextToken == null) {
                    checkForExpiredShards(currentTimeMs);
                }
            }
        }
    }

    private void initShardListing(long currentTimeMs) {
        if (currentTimeMs < nextListShardsTimeMs) {
            return;
        }
        listShardsResult = listAllShardsAsync(nextToken);
        nextListShardsTimeMs = currentTimeMs + listShardsRateTracker.next();
    }

    private Future<ListShardsResult> listAllShardsAsync(String nextToken) {
        ShardFilterType filterType = ShardFilterType.FROM_TRIM_HORIZON;
        //all shards within the retention period (including closed, excluding expired)

        ListShardsRequest request = listAllShardsRequest(streamName, nextToken, filterType);
        return kinesis.listShardsAsync(request);
    }

    public static ListShardsRequest listAllShardsRequest(
            String stream,
            @Nullable String nextToken,
            ShardFilterType filterType
    ) {
        ListShardsRequest request = new ListShardsRequest();
        if (nextToken == null) {
            request.setStreamName(stream);
        } else {
            request.setNextToken(nextToken);
        }

        //include all the shards within the retention period of the data stream
        request.setShardFilter(new ShardFilter().withType(filterType));

        return request;
    }

    private void checkForNewShards(long currentTimeMs, ListShardsResult result) {
        Set<Shard> shards = result.getShards().stream()
                .filter(shard -> KinesisUtil.shardBelongsToRange(shard, memberHashRange))
                .collect(Collectors.toSet());
        Map<Shard, Integer> newShards = shardTracker.markDetections(shards, currentTimeMs);

        if (!newShards.isEmpty()) {
            logger.info("New shards detected: " +
                    newShards.keySet().stream().map(Shard::getShardId).collect(joining(", ")));

            for (Map.Entry<Shard, Integer> e : newShards.entrySet()) {
                Shard shard = e.getKey();
                int owner = e.getValue();
                shardQueues[owner].addAdded(shard);
            }
        }
    }

    private void checkForExpiredShards(long currentTimeMs) {
        Map<String, Integer> expiredShards = shardTracker.removeExpiredShards(currentTimeMs);
        for (Map.Entry<String, Integer> e : expiredShards.entrySet()) {
            String shardId = e.getKey();
            int owner = e.getValue();
            logger.info("Expired shard detected: " + shardId);
            shardQueues[owner].addExpired(shardId);
        }
    }

    public void addKnownShard(String shardId, BigInteger startingHashKey) {
        shardTracker.addUndetected(shardId, startingHashKey, System.currentTimeMillis());
    }

    private void dealWithListShardsFailure(@Nonnull Exception failure) {
        nextToken = null;

        listShardRetryTracker.attemptFailed();
        if (listShardRetryTracker.shouldTryAgain()) {
            long timeoutMillis = listShardRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed listing shards, retrying in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextListShardsTimeMs = System.currentTimeMillis() + timeoutMillis;
        } else {
            throw rethrow(failure);
        }

    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int totalInstances) {
        // The maximum rate at which ListStreams operations can be performed on
        // a data stream is 100/second and we need to enforce this, even while
        // we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toMillis(1) * totalInstances,
                (int) (SHARD_LISTINGS_ALLOWED_PER_SECOND * RATIO_OF_SHARD_LISTING_RATE_UTILIZED));
    }

}
