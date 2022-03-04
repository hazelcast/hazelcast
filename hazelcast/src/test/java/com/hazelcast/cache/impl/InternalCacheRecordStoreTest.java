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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheFromDifferentNodesTest;
import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalCacheRecordStoreTest extends CacheTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

    @Override
    protected void onSetup() {
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return factory.newHazelcastInstance(createConfig());
    }

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.getSerializationConfig().addDataSerializableFactory(InternalCacheRecordStoreTestFactory.F_ID,
                new InternalCacheRecordStoreTestFactory());
        return config;
    }

    /**
     * Test for issue: https://github.com/hazelcast/hazelcast/issues/6618
     */
    @Test
    public void batchEventMapShouldBeCleanedAfterRemoveAll() {
        String cacheName = randomString();

        CacheConfig<Integer, String> config = createCacheConfig();
        CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener =
                new CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key = 1;
        String value = "value";
        cache.put(key, value);

        HazelcastInstance instance = ((HazelcastCacheManager) cacheManager).getHazelcastInstance();
        int partitionId = instance.getPartitionService().getPartition(key).getPartitionId();

        cache.removeAll();

        Node node = getNode(instance);
        assertNotNull(node);

        ICacheService cacheService = node.getNodeEngine().getService(ICacheService.SERVICE_NAME);
        AbstractCacheRecordStore recordStore = (AbstractCacheRecordStore) cacheService
                .getRecordStore("/hz/" + cacheName, partitionId);
        assertEquals(0, recordStore.batchEvent.size());
    }

    /**
     * Test for issue: https://github.com/hazelcast/hazelcast/issues/6983
     */
    @Test
    public void ownerStateShouldBeUpdatedAfterMigration() throws Exception {
        HazelcastCacheManager hzCacheManager = (HazelcastCacheManager) cacheManager;

        HazelcastInstance instance1 = hzCacheManager.getHazelcastInstance();
        Node node1 = getNode(instance1);
        NodeEngineImpl nodeEngine1 = node1.getNodeEngine();
        InternalPartitionService partitionService1 = nodeEngine1.getPartitionService();
        int partitionCount = partitionService1.getPartitionCount();

        String cacheName = randomName();
        CacheConfig cacheConfig = new CacheConfig().setName(cacheName);
        Cache<String, String> cache = cacheManager.createCache(cacheName, cacheConfig);
        String fullCacheName = hzCacheManager.getCacheNameWithPrefix(cacheName);

        for (int i = 0; i < partitionCount; i++) {
            String key = generateKeyForPartition(instance1, i);
            cache.put(key, "Value");
        }

        for (int i = 0; i < partitionCount; i++) {
            verifyPrimaryState(node1, fullCacheName, i, true);
        }

        HazelcastInstance instance2 = getHazelcastInstance();
        Node node2 = getNode(instance2);

        warmUpPartitions(instance1, instance2);
        waitAllForSafeState(instance1, instance2);

        for (int i = 0; i < partitionCount; i++) {
            boolean ownedByNode1 = partitionService1.isPartitionOwner(i);
            if (ownedByNode1) {
                verifyPrimaryState(node1, fullCacheName, i, true);
                verifyPrimaryState(node2, fullCacheName, i, false);
            } else {
                verifyPrimaryState(node1, fullCacheName, i, false);
                verifyPrimaryState(node2, fullCacheName, i, true);
            }
        }
    }

    private void verifyPrimaryState(Node node, String fullCacheName, int partitionId, boolean expectedState) throws Exception {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        Future<Boolean> isPrimaryOnNode = operationService.invokeOnTarget(
                ICacheService.SERVICE_NAME,
                createCacheOwnerStateGetterOperation(fullCacheName, partitionId),
                node.getThisAddress());
        assertEquals(expectedState, isPrimaryOnNode.get());
    }

    private CachePrimaryStateGetterOperation createCacheOwnerStateGetterOperation(String fullCacheName, int partitionId) {
        CachePrimaryStateGetterOperation operation = new CachePrimaryStateGetterOperation(fullCacheName);
        operation.setPartitionId(partitionId);
        operation.setValidateTarget(false);
        return operation;
    }

    static class CachePrimaryStateGetterOperation extends AbstractNamedOperation {

        private boolean isPrimary;

        private CachePrimaryStateGetterOperation() {
        }

        private CachePrimaryStateGetterOperation(String cacheName) {
            super(cacheName);
        }

        @Override
        public void run() throws Exception {
            ICacheService cacheService = getService();
            AbstractCacheRecordStore cacheRecordStore =
                    (AbstractCacheRecordStore) cacheService.getRecordStore(name, getPartitionId());
            isPrimary = cacheRecordStore.primary;
        }

        @Override
        public Object getResponse() {
            return isPrimary;
        }

        @Override
        public String getServiceName() {
            return ICacheService.SERVICE_NAME;
        }

        @Override
        public int getFactoryId() {
            return InternalCacheRecordStoreTestFactory.F_ID;
        }

        @Override
        public int getClassId() {
            return InternalCacheRecordStoreTestFactory.INTERNAL_CACHE_PRIMARY_STATE_GETTER;
        }
    }

    static class InternalCacheRecordStoreTestFactory implements DataSerializableFactory {

        static final int F_ID = 1;
        static final int INTERNAL_CACHE_PRIMARY_STATE_GETTER = 0;

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == INTERNAL_CACHE_PRIMARY_STATE_GETTER) {
                return new CachePrimaryStateGetterOperation();
            } else {
                throw new UnsupportedOperationException("Could not create instance of type ID " + typeId);
            }
        }
    }
}
