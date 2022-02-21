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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheLoadAllTest extends CacheTestSupport {

    private static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
    private HazelcastInstance[] hazelcastInstances;
    private HazelcastInstance hazelcastInstance;

    @Override
    protected void onSetup() {
        Config config = createConfig();
        hazelcastInstances = new HazelcastInstance[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            hazelcastInstances[i] = factory.newHazelcastInstance(config);
        }
        warmUpPartitions(hazelcastInstances);
        waitAllForSafeState(hazelcastInstances);
        hazelcastInstance = hazelcastInstances[0];
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
        hazelcastInstances = null;
        hazelcastInstance = null;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setBackupCount(INSTANCE_COUNT - 1);
        cacheConfig.setReadThrough(true);
        cacheConfig.setCacheLoaderFactory(FactoryBuilder.factoryOf(TestCacheLoader.class));
        return cacheConfig;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    private static String getValueOfKey(String key) {
        return "value_of_" + key;
    }

    private Map<String, String> createAndFillEntries() {
        final int ENTRY_COUNT_PER_PARTITION = 3;
        Node node = getNode(hazelcastInstance);
        int partitionCount = node.getPartitionService().getPartitionCount();
        Map<String, String> entries = new HashMap<String, String>(partitionCount * ENTRY_COUNT_PER_PARTITION);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int i = 0; i < ENTRY_COUNT_PER_PARTITION; i++) {
                String key = generateKeyForPartition(hazelcastInstance, partitionId);
                String value = getValueOfKey(key);
                entries.put(key, value);
            }
        }

        return entries;
    }

    @Test
    public void testLoadAll() throws InterruptedException {
        ICache<String, String> cache = createCache();
        String cacheName = cache.getName();
        Map<String, String> entries = createAndFillEntries();
        final CountDownLatch latch = new CountDownLatch(1);

        cache.loadAll(entries.keySet(), true, new CompletionListener() {
            @Override
            public void onCompletion() {
                latch.countDown();
            }

            @Override
            public void onException(Exception e) {
                latch.countDown();
            }
        });

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        // Verify that load-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        Node node = getNode(hazelcastInstance);
        InternalPartitionService partitionService = node.getPartitionService();
        SerializationService serializationService = node.getSerializationService();

        // Verify that backup of load-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            Data keyData = serializationService.toData(key);
            int keyPartitionId = partitionService.getPartitionId(keyData);
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                Node n = getNode(hazelcastInstances[i]);
                ICacheService cacheService = n.getNodeEngine().getService(ICacheService.SERVICE_NAME);
                ICacheRecordStore recordStore = cacheService.getRecordStore("/hz/" + cacheName, keyPartitionId);
                assertNotNull(recordStore);
                String actualValue = serializationService.toObject(recordStore.get(keyData, null));
                assertEquals(expectedValue, actualValue);
            }
        }
    }

    public static class TestCacheLoader implements CacheLoader<String, String> {

        @Override
        public String load(String key) throws CacheLoaderException {
            return getValueOfKey(key);
        }

        @Override
        public Map<String, String> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            Map<String, String> entries = new HashMap<String, String>();
            for (String key : keys) {
                entries.put(key, getValueOfKey(key));
            }
            return entries;
        }

    }

}
