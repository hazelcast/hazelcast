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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class ElasticSourcePSupplier<T> implements ProcessorSupplier {

    private static final long serialVersionUID = 1L;

    private final ElasticSourceConfiguration<T> configuration;

    private final List<Shard> shards;
    private Map<Integer, List<Shard>> shardsByProcessor;

    ElasticSourcePSupplier(@Nonnull ElasticSourceConfiguration<T> configuration,
                           @Nonnull List<Shard> shards) {
        this.configuration = requireNonNull(configuration);
        this.shards = requireNonNull(shards);
    }


    @Override
    public void init(@Nonnull Context context) {
        if (configuration.isCoLocatedReadingEnabled()) {
            if (configuration.isSlicingEnabled()) {
                shardsByProcessor = new HashMap<>();
                for (int i = 0; i < context.localParallelism(); i++) {
                    shardsByProcessor.put(i, shards);
                }
            } else {
                shardsByProcessor = Util.distributeObjects(context.localParallelism(), shards);
            }
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return IntStream.range(0, count)
                        .mapToObj(i -> configuration.isCoLocatedReadingEnabled() ?
                                new ElasticSourceP<>(configuration, shardsByProcessor.get(i)) :
                                new ElasticSourceP<>(configuration, emptyList()))
                        .collect(toList());
    }

}
