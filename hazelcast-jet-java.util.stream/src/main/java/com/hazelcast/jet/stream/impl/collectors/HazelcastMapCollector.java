/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.dag.tap.HazelcastSinkTap;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.dag.tap.SinkTap;
import com.hazelcast.jet.dag.tap.TapType;
import com.hazelcast.jet.stream.Distributed;

import java.util.function.Function;

import static com.hazelcast.jet.stream.impl.StreamUtil.MAP_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class HazelcastMapCollector<T, K, V> extends AbstractHazelcastCollector<T, IMap<K, V>> {

    protected final String mapName;
    protected final Function<? super T, ? extends K> keyMapper;
    protected final Function<? super T, ? extends V> valueMapper;


    public HazelcastMapCollector(Function<? super T, ? extends K> keyMapper,
                                 Function<? super T, ? extends V> valueMapper) {
        this(randomName(MAP_PREFIX), keyMapper, valueMapper);
    }

    public HazelcastMapCollector(String mapName, Function<? super T, ? extends K> keyMapper,
                                 Function<? super T, ? extends V> valueMapper) {
        this.mapName = mapName;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    protected IMap<K, V> getTarget(HazelcastInstance instance) {
        return instance.getMap(mapName);
    }

    @Override
    protected <U extends T> Distributed.Function<U, Tuple> toTupleMapper() {
        return v -> new JetTuple2<>(keyMapper.apply(v), valueMapper.apply(v));
    }

    @Override
    protected SinkTap getSinkTap() {
        return new HazelcastSinkTap(mapName, TapType.HAZELCAST_MAP);
    }

}
