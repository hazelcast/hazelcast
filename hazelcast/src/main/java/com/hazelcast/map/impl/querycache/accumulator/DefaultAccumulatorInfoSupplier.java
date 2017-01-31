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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Default implementation of {@link AccumulatorInfoSupplier}.
 * <p/>
 * At most one thread can write to this class at a time.
 *
 * @see AccumulatorInfoSupplier
 */
public class DefaultAccumulatorInfoSupplier implements AccumulatorInfoSupplier {

    private static final ConstructorFunction<String, ConcurrentMap<String, AccumulatorInfo>> INFO_CTOR
            = new ConstructorFunction<String, ConcurrentMap<String, AccumulatorInfo>>() {
        @Override
        public ConcurrentMap<String, AccumulatorInfo> createNew(String arg) {
            return new ConcurrentHashMap<String, AccumulatorInfo>();
        }
    };

    private final ConcurrentMap<String, ConcurrentMap<String, AccumulatorInfo>> cacheInfoPerMap;

    public DefaultAccumulatorInfoSupplier() {
        this.cacheInfoPerMap = new ConcurrentHashMap<String, ConcurrentMap<String, AccumulatorInfo>>();
    }

    @Override
    public AccumulatorInfo getAccumulatorInfoOrNull(String mapName, String cacheName) {
        ConcurrentMap<String, AccumulatorInfo> cacheToInfoMap = cacheInfoPerMap.get(mapName);
        return cacheToInfoMap.get(cacheName);
    }

    @Override
    public void putIfAbsent(String mapName, String cacheName, AccumulatorInfo info) {
        ConcurrentMap<String, AccumulatorInfo> cacheToInfoMap = getOrPutIfAbsent(cacheInfoPerMap, mapName, INFO_CTOR);
        cacheToInfoMap.putIfAbsent(cacheName, info);
    }

    @Override
    public void remove(String mapName, String cacheName) {
        ConcurrentMap<String, AccumulatorInfo> cacheToInfoMap = cacheInfoPerMap.get(mapName);
        if (cacheToInfoMap == null) {
            return;
        }
        cacheToInfoMap.remove(cacheName);
    }
}
