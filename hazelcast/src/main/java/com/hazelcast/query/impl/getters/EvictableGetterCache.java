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

package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

class EvictableGetterCache implements GetterCache {
    private final SampleableConcurrentHashMap<Class<?>, SampleableConcurrentHashMap<String, Getter>> getterCache;
    private final ConstructorFunction<Class<?>, SampleableConcurrentHashMap<String, Getter>> getterCacheConstructor;

    private final int maxClassCount;
    private final int afterEvictionClassCount;
    private final int maxGetterPerClassCount;
    private final int afterEvictionGetterPerClassCount;

    EvictableGetterCache(int maxClassCount, final int maxGetterPerClassCount, float evictPercentage, boolean strongReferences) {
        ReferenceType referenceType = strongReferences ? ReferenceType.STRONG : ReferenceType.SOFT;
        getterCache = new SampleableConcurrentHashMap<>(maxClassCount, referenceType, referenceType);
        getterCacheConstructor = arg -> new SampleableConcurrentHashMap<>(maxGetterPerClassCount);

        this.maxClassCount = maxClassCount;
        this.afterEvictionClassCount = (int) (maxClassCount * (1 - evictPercentage));
        this.maxGetterPerClassCount = maxGetterPerClassCount;
        this.afterEvictionGetterPerClassCount = (int) (maxGetterPerClassCount * (1 - evictPercentage));
    }

    @Nullable
    @Override
    public Getter getGetter(Class<?> clazz, String attributeName) {
        ConcurrentMap<String, Getter> cache = getterCache.get(clazz);
        if (cache == null) {
            return null;
        }
        return cache.get(attributeName);
    }

    @Override
    public Getter putGetter(Class<?> clazz, String attributeName, Getter getter) {
        SampleableConcurrentHashMap<String, Getter> cache = getOrPutIfAbsent(getterCache, clazz, getterCacheConstructor);
        Getter foundGetter = cache.putIfAbsent(attributeName, getter);
        evictOnPut(cache);
        return foundGetter == null ? getter : foundGetter;
    }

    private void evictOnPut(SampleableConcurrentHashMap<String, Getter> getterPerClassCache) {
        evictMap(getterPerClassCache, maxGetterPerClassCount, afterEvictionGetterPerClassCount);
        evictMap(getterCache, maxClassCount, afterEvictionClassCount);
    }

    /**
     * It works on best effort basis. If concurrent calls are involved, it may evict all elements, but it's unlikely.
     */
    private void evictMap(SampleableConcurrentHashMap<?, ?> map, int triggeringEvictionSize, int afterEvictionSize) {
        map.purgeStaleEntries();
        int mapSize = map.size();
        if (mapSize - triggeringEvictionSize >= 0) {
            for (SampleableConcurrentHashMap.SamplingEntry<?, ?> entry : map.getRandomSamples(mapSize - afterEvictionSize)) {
                map.remove(entry.getEntryKey());
            }
        }
    }

    int getClassCacheSize() {
        return getterCache.size();
    }

    int getGetterPerClassCacheSize(Class<?> clazz) {
        SampleableConcurrentHashMap<?, ?> cacheForClass = getterCache.get(clazz);
        return cacheForClass != null ? cacheForClass.size() : -1;
    }
}
