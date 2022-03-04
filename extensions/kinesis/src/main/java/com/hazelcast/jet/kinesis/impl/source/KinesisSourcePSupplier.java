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

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class KinesisSourcePSupplier<T> implements ProcessorSupplier {

    private static final long serialVersionUID = 2L;

    @Nonnull
    private final AwsConfig awsConfig;
    @Nonnull
    private final String stream;
    @Nonnull
    private final EventTimePolicy<? super T> eventTimePolicy;
    @Nonnull
    private final HashRange hashRange;
    @Nonnull
    private final RetryStrategy retryStrategy;
    @Nonnull
    private final InitialShardIterators initialShardIterators;
    @Nonnull
    private final BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn;

    private transient int memberCount;
    private transient ILogger logger;
    private transient AmazonKinesisAsync client;

    public KinesisSourcePSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull HashRange hashRange,
            @Nonnull RetryStrategy retryStrategy,
            @Nonnull InitialShardIterators initialShardIterators,
            @Nonnull BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.eventTimePolicy = eventTimePolicy;
        this.hashRange = hashRange;
        this.retryStrategy = retryStrategy;
        this.initialShardIterators = initialShardIterators;
        this.projectionFn = projectionFn;
    }

    @Override
    public void init(@Nonnull Context context) {
        memberCount = context.memberCount();
        logger = context.logger();
        client = awsConfig.buildClient();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        HashRange[] rangePartitions = rangePartitions(count);
        ShardQueue[] shardQueues = shardQueues(count);
        RangeMonitor rangeMonitor = new RangeMonitor(
                memberCount,
                client,
                stream,
                hashRange,
                rangePartitions,
                shardQueues,
                retryStrategy,
                logger
        );

        return IntStream.range(0, count)
                .mapToObj(i ->
                        new KinesisSourceP<>(
                                client,
                                stream,
                                eventTimePolicy,
                                hashRange,
                                rangePartitions[i],
                                shardQueues[i],
                                i == 0 ? rangeMonitor : null,
                                retryStrategy,
                                initialShardIterators,
                                projectionFn
                        ))
                .collect(toList());
    }

    private ShardQueue[] shardQueues(int count) {
        ShardQueue[] queues = new ShardQueue[count];
        Arrays.setAll(queues, IGNORED -> new ShardQueue());
        return queues;
    }

    private HashRange[] rangePartitions(int count) {
        HashRange[] ranges = new HashRange[count];
        Arrays.setAll(ranges, i -> hashRange.partition(i, count));
        return ranges;
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (client != null) {
            client.shutdown();
        }
    }
}
