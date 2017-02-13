/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.stream.IStreamMap;

import java.util.function.Function;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueMapName;

public class HazelcastMapCollector<T, K, V> extends AbstractHazelcastCollector<T, IStreamMap<K, V>> {

    protected final String mapName;
    protected final Function<? super T, ? extends K> keyMapper;
    protected final Function<? super T, ? extends V> valueMapper;


    public HazelcastMapCollector(Function<? super T, ? extends K> keyMapper,
                                 Function<? super T, ? extends V> valueMapper) {
        this(uniqueMapName(), keyMapper, valueMapper);
    }

    public HazelcastMapCollector(String mapName, Function<? super T, ? extends K> keyMapper,
                                 Function<? super T, ? extends V> valueMapper) {
        this.mapName = mapName;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    protected IStreamMap<K, V> getTarget(JetInstance instance) {
        return instance.getMap(mapName);
    }

    @Override
    protected ProcessorMetaSupplier getSinkSupplier() {
        return Processors.writeMap(mapName);
    }

    @Override
    protected int localParallelism() {
        return -1;
    }

    @Override
    protected String getName() {
        return mapName;
    }

}
