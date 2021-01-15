/*
 * Copyright 2020 Hazelcast Inc.
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
package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.hazelcast.jet.JetException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KinesisHelper {

    private final AmazonKinesisAsync kinesis;
    private final String stream;

    public KinesisHelper(AmazonKinesisAsync kinesis, String stream) {
        this.kinesis = kinesis;
        this.stream = stream;
    }

    public static boolean shardActive(@Nonnull Shard shard) {
        String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
        return endingSequenceNumber == null;
        //need to rely on this hack, because shard filters don't seem to work, on the mock at least ...
    }

    public static boolean shardBelongsToRange(@Nonnull Shard shard, @Nonnull HashRange range) {
        BigInteger startingHashKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
        return shardBelongsToRange(startingHashKey, range);
    }

    public static boolean shardBelongsToRange(@Nonnull BigInteger startingHashKey, @Nonnull HashRange range) {
        return range.contains(startingHashKey);
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

    public Future<DescribeStreamSummaryResult> describeStreamSummaryAsync() {
        DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
        request.setStreamName(stream);
        return kinesis.describeStreamSummaryAsync(request);
    }

    public Future<ListShardsResult> listAllShardsAsync(String nextToken) {
        ShardFilterType filterType = ShardFilterType.FROM_TRIM_HORIZON;
        //all shards within the retention period (including closed, excluding expired)

        ListShardsRequest request = listAllShardsRequest(stream, nextToken, filterType);
        return kinesis.listShardsAsync(request);
    }

    public Future<GetShardIteratorResult> getShardIteratorAsync(GetShardIteratorRequest request) {
        return kinesis.getShardIteratorAsync(request);
    }

    public Future<GetRecordsResult> getRecordsAsync(String shardIterator) {
        GetRecordsRequest request = new GetRecordsRequest();
        request.setShardIterator(shardIterator);
        return kinesis.getRecordsAsync(request);
    }

    public Future<PutRecordsResult> putRecordsAsync(Collection<PutRecordsRequestEntry> entries) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setRecords(entries);
        request.setStreamName(stream);
        return kinesis.putRecordsAsync(request);
    }

    public <T> T readResult(Future<T> future) throws Throwable {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Interrupted while waiting for results");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}
