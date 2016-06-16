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

package com.hazelcast.jet.stream;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.impl.MapDecorator;
import com.hazelcast.jet.stream.impl.StreamUtil;

/**
 * A decorator for {@link IMap} for supporting distributed {@link java.util.stream.Stream}
 * implementation.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public interface IStreamMap<K, V> extends IMap<K, V> {

    /**
     * Returns a parallel and distributed {@code Stream} with this list as its source.
     *
     * @return a parallel {@code Stream} over the elements in this collection
     * @since 1.8
     */
    DistributedStream<Entry<K, V>> stream();

    /**
     * Returns an {@link IMap} with {@link DistributedStream} support.
     *
     * @param map the Hazelcast map to decorate
     * @param <K> the type of keys maintained by this map
     * @param <V> the type of mapped values
     * @return Returns an {@link IMap} with {@link DistributedStream} support.
     */
    static <K, V> IStreamMap<K, V> streamMap(IMap<K, V> map) {
        return new MapDecorator<>(map, StreamUtil.getHazelcastInstance(map));
    }
}
