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
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceAccessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.Accessors.getMetricsRegistry;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachePutAllTest extends CacheTestSupport {

    private static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory = getInstanceFactory(INSTANCE_COUNT);
    private HazelcastInstance[] hazelcastInstances;
    private HazelcastInstance hazelcastInstance;

    protected TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        return createHazelcastInstanceFactory(instanceCount);
    }

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
        return cacheConfig;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    private Map<String, String> createAndFillEntries() {
        final int ENTRY_COUNT_PER_PARTITION = 3;
        Node node = getNode(hazelcastInstance);
        int partitionCount = node.getPartitionService().getPartitionCount();
        Map<String, String> entries = new HashMap<String, String>(partitionCount * ENTRY_COUNT_PER_PARTITION);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int i = 0; i < ENTRY_COUNT_PER_PARTITION; i++) {
                String key = generateKeyForPartition(hazelcastInstance, partitionId);
                String value = generateRandomString(16);
                entries.put(key, value);
            }
        }

        return entries;
    }

    @Test
    public void testPutAll() {
        ICache<String, String> cache = createCache();
        String cacheName = cache.getName();
        Map<String, String> entries = createAndFillEntries();

        cache.putAll(entries);

        // Verify that put-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        Node node = getNode(hazelcastInstance);
        InternalPartitionService partitionService = node.getPartitionService();
        SerializationService serializationService = node.getSerializationService();

        // Verify that backup of put-all works
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

    @Test
    public void testPutAllWithExpiration() {
        final long EXPIRATION_TIME_IN_MILLISECONDS = 5000;

        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(EXPIRATION_TIME_IN_MILLISECONDS, 0, 0);
        ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        cache.putAll(entries, expiryPolicy);

        sleepAtLeastMillis(EXPIRATION_TIME_IN_MILLISECONDS + 1000);

        // Verify that expiration of put-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String actualValue = cache.get(key);
            assertNull(actualValue);
        }
    }

    @Test
    public void testPutAll_whenEntryExpiresOnCreate() {
        Factory<? extends ExpiryPolicy> expiryPolicyFactory = FactoryBuilder.factoryOf(new CreatedExpiryPolicy(Duration.ZERO));
        CacheConfig<String, String> cacheConfig = new CacheConfig<String, String>();
        cacheConfig.setTypes(String.class, String.class);
        cacheConfig.setExpiryPolicyFactory(expiryPolicyFactory);
        cacheConfig.setStatisticsEnabled(true);
        cacheConfig.setBackupCount(1);

        Cache<String, String> cache = createCache(cacheConfig);

        String key = generateKeyOwnedBy(hazelcastInstance);
        // need to count the number of backup failures on backup member
        OperationService operationService = getOperationService(hazelcastInstances[1]);
        MetricsRegistry metricsRegistry = getMetricsRegistry(hazelcastInstances[1]);
        assertEquals(0L, OperationServiceAccessor.getFailedBackupsCount(hazelcastInstances[1]).get());

        Map<String, String> entries = new HashMap<String, String>();
        entries.put(key, randomString());
        cache.putAll(entries);

        assertNull(cache.get(key));
        // force collect metrics
        metricsRegistry.provideMetrics(operationService);
        assertEquals(0L, OperationServiceAccessor.getFailedBackupsCount(hazelcastInstances[1]).get());
    }
}
