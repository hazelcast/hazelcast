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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;

/**
 * A decorator for {@link IMap} that provides a distributed
 * {@link java.util.stream.Stream} implementation.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public interface IStreamMap<K, V> extends IMap<K, V> {

    /**
     * Returns a {@link DistributedStream} with this map as its source.
     * <p>
     * If the underlying map is being concurrently modified, there are no
     * guarantees given with respect to missing or duplicate items in a
     * stream operation.
     */
    DistributedStream<Entry<K, V>> stream();

    /**
     * Returns a {@link DistributedStream} with this map as its source.
     * Entries will be filtered and mapped according to the given predicate
     * and projection.
     * <p>
     * If the underlying map is being concurrently modified, there are no
     * guarantees given with respect to missing or duplicate items in a
     * stream operation.
     * <p>
     * To create a {@code Predicate} instance you might prefer to use Jet's
     * {@link com.hazelcast.jet.GenericPredicates}.
     */
    <T> DistributedStream<T> stream(@Nonnull Predicate<K, V> predicate,
                                    @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn);
}
