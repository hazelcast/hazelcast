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

package com.hazelcast.jet.elastic.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class ElasticSourcePMetaSupplier<T> implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_LOCAL_PARALLELISM = 2;

    @Nonnull
    private final ElasticSourceConfiguration<T> configuration;

    private transient Map<Address, List<Shard>> assignedShards;
    private transient Address ownerAddress;

    public ElasticSourcePMetaSupplier(@Nonnull ElasticSourceConfiguration<T> configuration) {
        this.configuration = configuration;
    }

    @Override
    public int preferredLocalParallelism() {
        if (configuration.isCoLocatedReadingEnabled() || configuration.isSlicingEnabled()) {
            return DEFAULT_LOCAL_PARALLELISM;
        } else {
            return 1;
        }
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        try (ElasticCatClient catClient = new ElasticCatClient(
                configuration.clientFn().get().getLowLevelClient(),
                configuration.retries()
        )) {
            List<Shard> shards = catClient.shards(configuration.searchRequestFn().get().indices());

            if (configuration.isCoLocatedReadingEnabled()) {
                Set<Address> addresses = context
                        .hazelcastInstance().getCluster().getMembers().stream()
                        .map(Member::getAddress)
                        .collect(toSet());
                assignedShards = assignShards(shards, addresses);
            } else {
                ownerAddress = context.hazelcastInstance().getPartitionService()
                                      .getPartition(context.jobId()).getOwner().getAddress();
                assignedShards = emptyMap();
            }
        }
    }

    static Map<Address, List<Shard>> assignShards(Collection<Shard> shards, Collection<Address> addresses) {
        Map<String, List<Shard>> nodeCandidates = shards.stream()
                                                        .collect(groupingBy(Shard::getIp));
        Map<Address, List<Shard>> nodeAssigned = new HashMap<>();

        if (!addresses.stream().map(Address::getHost).collect(toSet())
                      .containsAll(nodeCandidates.keySet())) {
            throw new JetException("Shard locations are not equal to Hazelcast members locations, " +
                    "shards=" + nodeCandidates.keySet() + ", Hazelcast members=" + addresses);
        }

        int uniqueShards = (int) shards.stream().map(Shard::indexShard).distinct().count();
        Set<String> assignedShards = new HashSet<>();

        int candidatesSize = nodeCandidates.size();
        int iterations = (uniqueShards + candidatesSize - 1) / candidatesSize; // Same as Math.ceil for float div
        for (int i = 0; i < iterations; i++) {
            for (Address address : addresses) {
                String host = address.getHost();
                List<Shard> thisNodeCandidates = nodeCandidates.getOrDefault(host, emptyList());
                if (thisNodeCandidates.isEmpty()) {
                    continue;
                }
                Shard shard = thisNodeCandidates.remove(0);

                List<Shard> nodeShards = nodeAssigned.computeIfAbsent(address, (key) -> new ArrayList<>());
                nodeShards.add(shard);

                nodeCandidates.values().forEach(candidates ->
                        candidates.removeIf(next -> next.indexShard().equals(shard.indexShard())));

                assignedShards.add(shard.indexShard());
            }
        }
        if (assignedShards.size() != uniqueShards) {
            throw new JetException("Not all shards have been assigned");
        }
        return nodeAssigned;
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (configuration.isSlicingEnabled() || configuration.isCoLocatedReadingEnabled()) {
            return address -> {
                List<Shard> shards = assignedShards.getOrDefault(address, emptyList());
                return new ElasticSourcePSupplier<>(configuration, shards);
            };
        } else {
            return address -> address.equals(ownerAddress) ? new ElasticSourcePSupplier<>(configuration, emptyList())
                    : count -> nCopies(count, Processors.noopP().get());
        }
    }

}
