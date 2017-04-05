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

package com.hazelcast.jet.stream;

import com.hazelcast.cache.ICache;

import java.util.Map;


/**
 * A decorator for {@link ICache} for supporting distributed {@link java.util.stream.Stream}
 * implementation.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public interface IStreamCache<K, V> extends ICache<K, V> {

    /**
     * Returns a parallel and distributed {@code Stream} with this list as its source.
     * <p>
     * If the underlying cache is concurrently being modified, there are no guarantees
     * given with respect to missing or duplicate items in a stream operation.
     *
     * @return a parallel {@code Stream} over the elements in this collection
     * @since 1.8
     */
    DistributedStream<Map.Entry<K, V>> stream();

}
