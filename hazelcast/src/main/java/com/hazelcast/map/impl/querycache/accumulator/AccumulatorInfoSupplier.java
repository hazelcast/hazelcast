/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.accumulator;

import java.util.concurrent.ConcurrentMap;

/**
 * Supplies {@link AccumulatorInfo} according to name of {@code IMap} and
 * name of {@code QueryCache}.
 */
public interface AccumulatorInfoSupplier {

    /**
     * Returns {@link AccumulatorInfo} for cache of ma{@code IMap}p.
     *
     * @param mapName map name.
     * @param cacheId cache name.
     * @return {@link AccumulatorInfo} for cache of map.
     */
    AccumulatorInfo getAccumulatorInfoOrNull(String mapName, String cacheId);

    /**
     * Adds a new {@link AccumulatorInfo} for the query-cache of {@code IMap}.
     *
     * @param mapName map name.
     * @param cacheId cache name.
     */
    void putIfAbsent(String mapName, String cacheId, AccumulatorInfo info);

    /**
     * Removes {@link AccumulatorInfo} from this supplier.
     *
     * @param mapName map name.
     * @param cacheId cache name.
     */
    void remove(String mapName, String cacheId);

    /**
     * @return all {@link AccumulatorInfo} of all {@code QueryCache} by map name
     */
    ConcurrentMap<String, ConcurrentMap<String, AccumulatorInfo>> getAll();
}
