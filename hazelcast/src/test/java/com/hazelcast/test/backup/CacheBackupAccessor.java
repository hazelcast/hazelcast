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

package com.hazelcast.test.backup;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.serialization.SerializationService;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.getHazelcastInstanceImpl;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.TestTaskExecutorUtil.runOnPartitionThread;

/**
 * Implementation of {@link BackupAccessor} for {@link ICache}.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
class CacheBackupAccessor<K, V> extends AbstractBackupAccessor<K, V> implements BackupAccessor<K, V> {

    private final String cacheName;

    CacheBackupAccessor(HazelcastInstance[] cluster, String cacheName, int replicaIndex) {
        super(cluster, replicaIndex);
        this.cacheName = cacheName;
    }

    @Override
    public int size() {
        InternalPartitionService partitionService = getNode(cluster[0]).getPartitionService();
        IPartition[] partitions = partitionService.getPartitions();
        int count = 0;
        for (IPartition partition : partitions) {
            Address replicaAddress = partition.getReplicaAddress(replicaIndex);
            if (replicaAddress == null) {
                continue;
            }

            HazelcastInstance hz = getInstanceWithAddress(replicaAddress);
            HazelcastInstanceImpl hazelcastInstanceImpl = getHazelcastInstanceImpl(hz);
            CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstanceImpl);
            HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();

            NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
            CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);
            String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(cacheName);
            int partitionId = partition.getPartitionId();

            count += runOnPartitionThread(hz, new SizeCallable(cacheService, cacheNameWithPrefix, partitionId), partitionId);
        }
        return count;
    }

    @Override
    public V get(K key) {
        IPartition partition = getPartitionForKey(key);
        HazelcastInstance hz = getHazelcastInstance(partition);

        Node node = getNode(hz);
        SerializationService serializationService = node.getSerializationService();
        CacheService cacheService = node.getNodeEngine().getService(CacheService.SERVICE_NAME);
        String cacheNameWithPrefix = getCacheNameWithPrefix(hz, cacheName);
        int partitionId = partition.getPartitionId();

        return runOnPartitionThread(hz, new GetValueCallable(serializationService, cacheService, cacheNameWithPrefix, partitionId,
                key), partitionId);
    }

    ExpiryPolicy getExpiryPolicy(K key) {
        InternalPartition partition = getPartitionForKey(key);
        HazelcastInstance hz = getHazelcastInstance(partition);

        Node node = getNode(hz);
        SerializationService serializationService = node.getSerializationService();
        CacheService cacheService = node.getNodeEngine().getService(CacheService.SERVICE_NAME);
        String cacheNameWithPrefix = getCacheNameWithPrefix(hz, cacheName);
        int partitionId = partition.getPartitionId();

        return runOnPartitionThread(hz, new GetExpiryPolicyCallable(serializationService, cacheService, cacheNameWithPrefix,
                partitionId, key), partitionId);
    }

    private static String getCacheNameWithPrefix(HazelcastInstance hz, String cacheName) {
        HazelcastInstanceImpl hazelcastInstanceImpl = getHazelcastInstanceImpl(hz);
        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstanceImpl);
        HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();
        return cacheManager.getCacheNameWithPrefix(cacheName);
    }

    private static class SizeCallable extends AbstractClassLoaderAwareCallable<Integer> {

        private final CacheService cacheService;
        private final String cacheNameWithPrefix;
        private final int partitionId;

        SizeCallable(CacheService cacheService, String cacheNameWithPrefix, int partitionId) {
            this.cacheService = cacheService;
            this.cacheNameWithPrefix = cacheNameWithPrefix;
            this.partitionId = partitionId;
        }

        @Override
        public Integer callInternal() {
            ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, partitionId);
            if (recordStore == null) {
                return 0;
            }
            return recordStore.size();
        }
    }

    private class GetValueCallable extends AbstractClassLoaderAwareCallable<V> {

        private final SerializationService serializationService;
        private final CacheService cacheService;
        private final String cacheNameWithPrefix;
        private final int partitionId;
        private final K key;

        GetValueCallable(SerializationService serializationService, CacheService cacheService, String cacheNameWithPrefix,
                         int partitionId, K key) {
            this.serializationService = serializationService;
            this.cacheService = cacheService;
            this.cacheNameWithPrefix = cacheNameWithPrefix;
            this.partitionId = partitionId;
            this.key = key;
        }

        @Override
        public V callInternal() {
            ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, partitionId);
            if (recordStore == null) {
                return null;
            }
            Data keyData = serializationService.toData(key);
            Map<Data, CacheRecord> records = recordStore.getReadOnlyRecords();
            CacheRecord cacheRecord = records.get(keyData);
            if (cacheRecord == null) {
                return null;
            }
            Object value = cacheRecord.getValue();
            return serializationService.toObject(value);
        }
    }

    private class GetExpiryPolicyCallable extends AbstractClassLoaderAwareCallable<ExpiryPolicy> {

        private final SerializationService serializationService;
        private final CacheService cacheService;
        private final String cacheNameWithPrefix;
        private final int partitionId;
        private final K key;

        GetExpiryPolicyCallable(SerializationService serializationService, CacheService cacheService, String cacheNameWithPrefix,
                                int partitionId, K key) {
            this.serializationService = serializationService;
            this.cacheService = cacheService;
            this.cacheNameWithPrefix = cacheNameWithPrefix;
            this.partitionId = partitionId;
            this.key = key;
        }

        @Override
        public ExpiryPolicy callInternal() {
            ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, partitionId);
            if (recordStore == null) {
                return null;
            }
            Data keyData = serializationService.toData(key);
            CacheRecord cacheRecord = recordStore.getReadOnlyRecords().get(keyData);
            if (cacheRecord == null) {
                return null;
            }
            Object expiryPolicy = cacheRecord.getExpiryPolicy();
            return serializationService.toObject(expiryPolicy);
        }
    }
}
