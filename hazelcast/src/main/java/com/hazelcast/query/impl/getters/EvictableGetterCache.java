/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.SampleableConcurrentHashMap;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

class EvictableGetterCache {

    private final SampleableConcurrentHashMap<Class, SampleableConcurrentHashMap<String, Getter>> getterCache;
    private final ConstructorFunction<Class, SampleableConcurrentHashMap<String, Getter>> getterCacheConstructor;

    private final int afterEvictionClassCount;
    private final int afterEvictionGetterPerClassCount;

    EvictableGetterCache(int maxClassCount, final int maxGetterPerClassCount, float loadFactor) {
        getterCache = new SampleableConcurrentHashMap<Class, SampleableConcurrentHashMap<String, Getter>>(maxClassCount);
        getterCacheConstructor = new ConstructorFunction<Class, SampleableConcurrentHashMap<String, Getter>>() {
            @Override
            public SampleableConcurrentHashMap<String, Getter> createNew(Class arg) {
                return new SampleableConcurrentHashMap<String, Getter>(maxGetterPerClassCount);
            }
        };
        afterEvictionClassCount = (int) (maxClassCount * loadFactor);
        afterEvictionGetterPerClassCount = (int) (maxGetterPerClassCount * loadFactor);
    }

    @Nullable
    Getter getGetter(Class clazz, String attribute) {
        ConcurrentMap<String, Getter> cache = getterCache.get(clazz);
        if (cache == null) {
            return null;
        }
        return cache.get(attribute);
    }

    Getter putGetter(Class clazz, String attribute, Getter getter) {
        SampleableConcurrentHashMap<String, Getter> cache = getOrPutIfAbsent(getterCache, clazz, getterCacheConstructor);
        Getter foundGetter = cache.putIfAbsent(attribute, getter);
        evictOnPut(cache);
        return foundGetter == null ? getter : foundGetter;
    }

    private void evictOnPut(SampleableConcurrentHashMap<String, Getter> getterPerClassCache) {
        evict(getterPerClassCache, afterEvictionGetterPerClassCount);
        evict(getterCache, afterEvictionClassCount);
    }

    private void evict(SampleableConcurrentHashMap<?, ?> map, int expectedSize) {
        int evictCount = map.size() - expectedSize;
        if (evictCount > 0) {
            Iterable<SampleableConcurrentHashMap.SamplingEntry> it = map.getRandomSamples(evictCount);
            for (SampleableConcurrentHashMap.SamplingEntry entry : it) {
                map.remove(entry.getKey());
            }
        }
    }

}
