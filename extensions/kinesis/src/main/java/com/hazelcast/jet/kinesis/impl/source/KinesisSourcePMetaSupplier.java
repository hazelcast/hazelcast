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

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class KinesisSourcePMetaSupplier<T> implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 2L;

    @Nonnull
    private final AwsConfig awsConfig;
    @Nonnull
    private final String stream;
    @Nonnull
    private final RetryStrategy retryStrategy;
    @Nonnull
    private final InitialShardIterators initialShardIterators;
    @Nonnull
    private final EventTimePolicy<? super T> eventTimePolicy;
    @Nonnull
    private final BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn;

    private transient Map<Address, HashRange> assignedHashRanges;

    public KinesisSourcePMetaSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull RetryStrategy retryStrategy,
            @Nonnull InitialShardIterators initialShardIterators,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.retryStrategy = retryStrategy;
        this.initialShardIterators = initialShardIterators;
        this.eventTimePolicy = eventTimePolicy;
        this.projectionFn = projectionFn;
    }

    @Override
    public void init(@Nonnull ProcessorMetaSupplier.Context context) {
        List<Address> addresses = getMemberAddresses(context);
        assignedHashRanges = assignHashRangesToMembers(addresses);
        if (context.logger().isFineEnabled()) {
            context.logger().fine("Hash ranges assigned to members: \n\t" +
                    assignedHashRanges.entrySet().stream().map(Object::toString).collect(joining("\n\t")));
        }
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        return address -> {
            HashRange assignedRange = assignedHashRanges.get(address);
            return new KinesisSourcePSupplier<>(awsConfig, stream, eventTimePolicy,
                    assignedRange, retryStrategy, initialShardIterators, projectionFn);
        };
    }

    @Nonnull
    private static List<Address> getMemberAddresses(@Nonnull Context context) {
        return context
                .hazelcastInstance().getCluster().getMembers().stream()
                .map(Member::getAddress)
                .collect(toList());
    }

    /**
     * Divide the range of all possible hash key values into equally sized
     * chunks, as many as there are Jet members in the cluster and assign each
     * chunk to a member.
     */
    @Nonnull
    private static Map<Address, HashRange> assignHashRangesToMembers(List<Address> addresses) {
        Map<Address, HashRange> addressRanges = new HashMap<>();
        for (int i = 0; i < addresses.size(); i++) {
            Address address = addresses.get(i);
            addressRanges.put(address, HashRange.DOMAIN.partition(i, addresses.size()));
        }
        return addressRanges;
    }
}
