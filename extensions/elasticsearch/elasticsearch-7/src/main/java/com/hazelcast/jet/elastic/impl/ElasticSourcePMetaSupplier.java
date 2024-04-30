/*
 * Copyright 2024 Hazelcast Inc.
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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.getOwnerAddress;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static java.util.Comparator.comparingInt;
import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ElasticSourcePMetaSupplier<T> implements ProcessorMetaSupplier {

    @Serial
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
                Set<Address> addresses = context.partitionAssignment().keySet();
                assignedShards = assignShards(shards, addresses);
            } else {
                ownerAddress = getOwnerAddress(context, context.jobId());
                assignedShards = emptyMap();
            }
        }
    }

    /**
     * Supports multiple nodes on the same host. Shards are distributed evenly across hosts (not nodes).
     */
    static Map<Address, List<Shard>> assignShards(Collection<Shard> shards, Collection<Address> addresses) {
        Map<String, List<String>> assignment = addresses.stream()  // host -> [indexShard...]
                .map(Address::getHost).distinct().collect(toMap(identity(), a -> new ArrayList<>()));
        Map<String, List<String>> nodeCandidates = shards.stream()  // indexShard -> [host...]
                .collect(groupingBy(Shard::indexShard, mapping(Shard::getIp, toList())));

        // Make the assignment
        nodeCandidates.forEach((indexShard, hosts) -> hosts.stream()
                .map(assignment::get)
                .filter(Objects::nonNull)
                .min(comparingInt(List::size))
                .orElseThrow(() -> new IllegalStateException("Selected members do not contain shard '" + indexShard + "'"))
                .add(indexShard));

        // Transform the results
        Map<String, List<Address>> addressMap = addresses.stream().collect(groupingBy(Address::getHost, toList()));
        Map<String, Shard> shardMap = shards.stream().collect(toMap(s -> s.indexShard() + "@" + s.getIp(), identity()));
        return assignment.entrySet().stream()
                .flatMap(e -> {
                    List<Address> a = addressMap.get(e.getKey());
                    List<Shard> s = e.getValue().stream()
                            .map(indexShard -> shardMap.get(indexShard + "@" + e.getKey())).toList();
                    int c = (int) Math.ceil((double) s.size() / a.size());
                    return IntStream.range(0, a.size())
                            .mapToObj(i -> entry(a.get(i), List.copyOf(s.subList(i * c, Math.min((i + 1) * c, s.size())))));
                }).collect(toMap(Entry::getKey, Entry::getValue));
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

    @Override
    public boolean closeIsCooperative() {
        return true;
    }
}
