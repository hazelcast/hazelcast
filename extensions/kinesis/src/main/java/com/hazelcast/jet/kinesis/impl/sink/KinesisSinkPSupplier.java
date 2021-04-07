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

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class KinesisSinkPSupplier<T> implements ProcessorSupplier {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final AwsConfig awsConfig;
    @Nonnull
    private final String stream;
    @Nonnull
    private final FunctionEx<T, String> keyFn;
    @Nonnull
    private final FunctionEx<T, byte[]> valueFn;
    @Nonnull
    private final RetryStrategy retryStrategy;

    private transient AmazonKinesisAsync client;
    private transient int memberCount;
    private transient ILogger logger;

    public KinesisSinkPSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn,
            @Nonnull RetryStrategy retryStrategy
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.keyFn = keyFn;
        this.valueFn = valueFn;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void init(@Nonnull Context context) {
        this.memberCount = context.memberCount();
        this.logger = context.logger();
        this.client = awsConfig.buildClient();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        // only the 0th processor will monitor the shard count for real, others will just read its counter
        ShardCountMonitorImpl shardCounter =
                new ShardCountMonitorImpl(memberCount, client, stream, retryStrategy, logger);
        NoopShardCountMonitor noopShardCounter =
                new NoopShardCountMonitor(shardCounter.getSharedShardCounter());

        return IntStream.range(0, count)
                .mapToObj(i -> new KinesisSinkP<>(
                        client,
                        stream,
                        keyFn,
                        valueFn,
                        i == 0 ? shardCounter : noopShardCounter,
                        retryStrategy
                ))
                .collect(toList());
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (client != null) {
            client.shutdown();
        }
    }
}
