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
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheExpirationBouncingMemberTest extends HazelcastTestSupport {

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

    @Test
    public void backups_should_be_empty_after_expiration() {
        Runnable[] methods = new Runnable[2];
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        final CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(testDriver);
        provider.getCacheManager().createCache(cacheName, getCacheConfig());
        HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();
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
                int length = members.length();
                for (int i = 0; i < length; i++) {
                    HazelcastInstance node = members.get(i);
                    assert node != null;
                    if (node.getLifecycleService().isRunning()
                            && node.getCluster().getClusterState() != ClusterState.PASSIVE) {

                        ClusterState clusterState = node.getCluster().getClusterState();
                        CacheService cacheService = getNodeEngineImpl(node).getService(CacheService.SERVICE_NAME);
                        IPartitionService partitionService = getNodeEngineImpl(node).getPartitionService();
                        int partitionCount = partitionService.getPartitionCount();

                        for (int j = 0; j < partitionCount; j++) {
                            ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, j);
                            if (recordStore == null) {
                                continue;
                            }
                            boolean isOwner = partitionService.getPartitionOwner(j).equals(getNodeEngineImpl(node).getThisAddress());
                            int recordStoreSize = recordStore.size();
                            String msg = "Failed on node: %s current cluster state is: %s, partitionId: %d"
                                    + "ownedEntryCount: %d, isOwner: %b";
                            String formattedMsg = String.format(msg, node.getName(), clusterState, j, recordStoreSize, isOwner);
                            assertEquals(formattedMsg, 0, recordStoreSize);

                        }
                    }
                }
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
