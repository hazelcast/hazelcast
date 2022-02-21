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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AdvancedCacheJournalTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 100;

    private HazelcastInstance[] instances;

    @Before
    public void init() {
        instances = createHazelcastInstanceFactory().newInstances(getConfig(), 4);
        warmUpPartitions(instances);
    }

    @Override
    protected Config getConfig() {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig().setName("*");
        cacheConfig.getEvictionConfig().setSize(Integer.MAX_VALUE);
        cacheConfig.setEventJournalConfig(new EventJournalConfig().setEnabled(true));

        return super.getConfig()
                    .setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT))
                    .addCacheConfig(cacheConfig);
    }

    @Test
    public void testBackupSafety() throws Exception {
        String name = randomMapName();
        Cache<Object, Object> cache = getCache(instances[0], name);
        int keyCount = 1000;
        int updateCount = 3;

        for (int n = 0; n < updateCount; n++) {
            for (int i = 0; i < keyCount; i++) {
                cache.put(i, randomString());
            }
        }

        LinkedList<HazelcastInstance> instanceList = new LinkedList<>(Arrays.asList(instances));
        waitAllForSafeState(instanceList);

        int expectedSize = keyCount * updateCount;
        while (instanceList.size() > 1) {
            HazelcastInstance instance = instanceList.removeFirst();
            instance.getLifecycleService().terminate();
            waitAllForSafeState(instanceList);

            cache = getCache(instanceList.getFirst(), name);
            int journalSize = getJournalSize(cache);
            assertEquals(expectedSize, journalSize);
        }
    }

    private Cache<Object, Object> getCache(HazelcastInstance instance, String name) {
        CacheManager cacheManager = createServerCachingProvider(instance).getCacheManager();
        return cacheManager.getCache(name);
    }

    private static <K, V> int getJournalSize(Cache<K, V> cache) throws ExecutionException, InterruptedException {
        int total = 0;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            CompletionStage<ReadResultSet<Object>> future =
                    ((CacheProxy<K, V>) cache).readFromEventJournal(0, 0, 10000, i, null, null);
            ReadResultSet<Object> resultSet = future.toCompletableFuture().get();
            int readCount = resultSet.readCount();
            total += readCount;
        }
        return total;
    }
}
