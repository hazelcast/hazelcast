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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheBackupTest extends HazelcastTestSupport {

    private void entrySuccessfullyRetrievedFromBackup(int backupCount, boolean sync) {
        final String KEY = "key";
        final String VALUE = "value";

        final int nodeCount = backupCount + 1;
        final TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = instanceFactory.newHazelcastInstance();
        }
        final HazelcastInstance hz = instances[0];

        final CachingProvider cachingProvider = createServerCachingProvider(hz);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final String cacheName = randomName();
        final CacheConfig cacheConfig = new CacheConfig().setName(cacheName);
        if (sync) {
            cacheConfig.setBackupCount(backupCount);
        } else {
            cacheConfig.setAsyncBackupCount(backupCount);
        }
        final Cache cache = cacheManager.createCache(cacheName, cacheConfig);

        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        cache.put(KEY, VALUE);

        final Node node = getNode(hz);
        final InternalPartitionService partitionService = node.getPartitionService();
        final int keyPartitionId = partitionService.getPartitionId(KEY);

        for (int i = 1; i <= backupCount; i++) {
            final Node backupNode = getNode(instances[i]);
            final SerializationService serializationService = backupNode.getSerializationService();
            final ICacheService cacheService = backupNode.getNodeEngine().getService(ICacheService.SERVICE_NAME);
            if (sync) {
                checkSavedRecordOnBackup(KEY, VALUE, cacheName, keyPartitionId, serializationService, cacheService);
            } else {
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        checkSavedRecordOnBackup(KEY, VALUE, cacheName, keyPartitionId, serializationService, cacheService);
                    }
                });
            }
        }
    }

    private void checkSavedRecordOnBackup(String key, String expectedValue, String cacheName, int keyPartitionId,
                                          SerializationService serializationService, ICacheService cacheService) {
        ICacheRecordStore recordStore = cacheService.getRecordStore("/hz/" + cacheName, keyPartitionId);
        assertNotNull(recordStore);
        assertEquals(expectedValue,
                serializationService.toObject(recordStore.get(serializationService.toData(key), null)));
    }

    @Test
    public void entrySuccessfullyRetrievedFromBackupWhenThereIsOneSyncBackup() {
        entrySuccessfullyRetrievedFromBackup(1, true);
    }

    @Test
    public void entrySuccessfullyRetrievedFromBackupWhenThereIsOneAsyncBackup() {
        entrySuccessfullyRetrievedFromBackup(1, false);
    }

    @Test
    public void entrySuccessfullyRetrievedFromBackupWhenThereIsTwoSyncBackup() {
        entrySuccessfullyRetrievedFromBackup(2, true);
    }

    @Test
    public void entrySuccessfullyRetrievedFromBackupWhenThereIsTwoAsyncBackup() {
        entrySuccessfullyRetrievedFromBackup(2, false);
    }

}
