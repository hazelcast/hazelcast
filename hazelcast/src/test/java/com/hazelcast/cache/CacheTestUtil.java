/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

import javax.cache.spi.CachingProvider;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;

public final class CacheTestUtil {

    private CacheTestUtil() {
    }

    /**
     * Returns the backup entries of an {@link ICache} by a given cache name.
     *
     * @param instances the {@link HazelcastInstance} array to gather the data from
     * @param cacheName the cache name
     * @param <K>       type of the key
     * @param <V>       type of the value
     * @return a {@link Map} with the backup entries
     */
    public static <K, V> Map<K, V> getBackupCache(HazelcastInstance[] instances, String cacheName) {
        Map<K, V> map = new HashMap<K, V>();
        for (HazelcastInstance instance : instances) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
            CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            SerializationService serializationService = nodeEngine.getSerializationService();

            CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(instance);
            HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();
            String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(cacheName);

            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                if (partitionService.isPartitionOwner(partitionId)) {
                    continue;
                }
                ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, partitionId);
                if (recordStore == null) {
                    continue;
                }
                for (Map.Entry<Data, CacheRecord> entry : recordStore.getReadOnlyRecords().entrySet()) {
                    CacheRecord record = entry.getValue();
                    K key = serializationService.toObject(entry.getKey());
                    V value = serializationService.toObject(record.getValue());
                    map.put(key, value);
                }
            }
        }
        return map;
    }
}
