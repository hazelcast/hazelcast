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

package com.hazelcast.internal.eviction;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.spi.CachingProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheExpirationBouncingMemberTest extends HazelcastTestSupport {

    private static final long FIVE_MINUNTES = 5 * 60 * 1000;

    private String cacheName = "test";
    private int backupCount = 3;
    private int keySpace = 1000;
    private String cacheNameWithPrefix;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .build();

    protected CacheConfig getCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(backupCount);
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new HazelcastExpiryPolicy(1, 1, 1)));
        return cacheConfig;
    }

    @Test(timeout = FIVE_MINUNTES)
    public void backups_should_be_empty_after_expiration() {
        Runnable[] methods = new Runnable[2];
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        final CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(testDriver);
        provider.getCacheManager().createCache(cacheName, getCacheConfig());
        final HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();
        cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(cacheName);

        methods[0] = new Get(provider);
        methods[1] = new Set(provider);

        bounceMemberRule.testRepeatedly(methods, 20);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                AtomicReferenceArray<HazelcastInstance> members = bounceMemberRule.getMembers();
                AtomicReferenceArray<HazelcastInstance> testDrivers = bounceMemberRule.getTestDrivers();

                assertSize(members);
                assertSize(testDrivers);
            }

            private void assertSize(AtomicReferenceArray<HazelcastInstance> members) {
                Map<Address, Map<Integer, Integer>> addressEntriesMap = new HashMap<Address, Map<Integer, Integer>>();
                int length = members.length();
                for (int i = 0; i < length; i++) {
                    HazelcastInstance node = members.get(i);
                    assert node != null;
                    if (node.getLifecycleService().isRunning()
                            && node.getCluster().getClusterState() != ClusterState.PASSIVE) {
                        CacheService cacheService = getNodeEngineImpl(node).getService(CacheService.SERVICE_NAME);
                        IPartitionService partitionService = getNodeEngineImpl(node).getPartitionService();
                        int partitionCount = partitionService.getPartitionCount();

                        for (int j = 0; j < partitionCount; j++) {
                            ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, j);
                            if (recordStore == null) {
                                continue;
                            }
                            int recordStoreSize = recordStore.size();

                            if (recordStoreSize > 0) {
                                addInfo(addressEntriesMap, getNodeEngineImpl(node).getThisAddress(), recordStore.getPartitionId(), recordStoreSize);
                            }
                        }
                    }
                }

                assertTrue(nonEmptyPartitionsInfo(addressEntriesMap), addressEntriesMap.isEmpty());
            }


            private void addInfo(Map<Address, Map<Integer, Integer>> map, Address address, int partitionId, int count) {
                Map<Integer, Integer> partitionIdEntryMap = map.get(address);
                if (partitionIdEntryMap == null) {
                    partitionIdEntryMap = new HashMap<Integer, Integer>();
                    map.put(address, partitionIdEntryMap);
                }
                partitionIdEntryMap.put(partitionId, count);
            }

            private String nonEmptyPartitionsInfo(Map<Address, Map<Integer, Integer>> addressEntriesMap) {
                HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
                InternalPartitionService partitionService = getPartitionService(steadyMember);
                PartitionTableView partitionTableView = partitionService.createPartitionTableView();
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<Address, Map<Integer, Integer>> addressMapEntry : addressEntriesMap.entrySet()) {
                    for (Map.Entry<Integer, Integer> partitionIdRecordsEntry : addressMapEntry.getValue().entrySet()) {
                        Address address = addressMapEntry.getKey();
                        int partitionId = partitionIdRecordsEntry.getKey();
                        int entryCount = partitionIdRecordsEntry.getValue();
                        Address ownerAddress = partitionTableView.getAddress(partitionId, 0);
                        builder.append(String.format("On %s: partition %d -> remaining entry count %d (owned by %s)\n",
                                address, partitionId, entryCount,
                                ownerAddress.equals(address) ? "this" : ownerAddress));
                    }
                }
                return builder.toString();
            }
        }, 240);
    }

    private class Get implements Runnable {

        private final Cache<Integer, Integer> cache;

        public Get(CachingProvider hz) {
            this.cache = hz.getCacheManager().getCache(cacheName);
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.get(i);
            }
        }
    }

    private class Set implements Runnable {

        private final Cache<Integer, Integer> cache;

        public Set(CachingProvider hz) {
            this.cache = hz.getCacheManager().getCache(cacheName);
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.put(i, i);
            }
        }
    }
}
