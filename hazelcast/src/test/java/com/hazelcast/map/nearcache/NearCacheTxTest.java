/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static java.lang.String.valueOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheTxTest extends NearCacheTestSupport {

    @Parameterized.Parameter
    public boolean batchInvalidationEnabled;

    @Parameterized.Parameters(name = "batchInvalidationEnabled:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    @Test
    public void test_whenEmptyMap_thenPopulatedNearCacheShouldReturnNull_neverNULL_OBJECT() {
        int size = 1;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        // populate map
        TransactionContext transactionContext = instance.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMap<Integer, Integer> map = transactionContext.getMap(mapName);
        for (int i = 0; i < size; i++) {
            // populate Near Cache
            assertNull(map.get(i));
            // fetch value from Near Cache
            assertNull(map.get(i));
        }
        transactionContext.commitTransaction();
    }

    @Test
    public void testBasicUsage() {
        int clusterSize = 3;
        int mapSize = 5000;
        final String mapName = "testBasicUsage";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);

        HazelcastInstance[] instances = factory.newInstances(config);

        TransactionContext transactionContext = getTransactionContext(instances);
        TransactionalMap<Integer, Integer> map = transactionContext.getMap(mapName);
        populateMap(map, mapSize);
        transactionContext.commitTransaction();

        for (HazelcastInstance instance : instances) {
            IMap<Integer, Integer> instanceMap = instance.getMap(mapName);
            for (int i = 0; i < mapSize; i++) {
                assertNotNull(instanceMap.get(i));
            }
        }

        transactionContext = getTransactionContext(instances);
        map = transactionContext.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i * 2);
        }
        transactionContext.commitTransaction();

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < mapSize; i++) {
                assertNotNull(m.get(i));
            }
        }

        for (HazelcastInstance instance : instances) {
            assertNearCacheNotEmpty(mapName, instance);
        }

        transactionContext = getTransactionContext(instances);
        map = transactionContext.getMap(mapName);
        for (Integer key : map.keySet()) {
            map.remove(key);
        }
        transactionContext.commitTransaction();
        for (HazelcastInstance instance : instances) {
            assertNearCacheEmptyEventually(mapName, instance);
        }
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), valueOf(batchInvalidationEnabled));
        return config;
    }

    private TransactionContext getTransactionContext(HazelcastInstance[] instances) {
        TransactionContext transactionContext = instances[0].newTransactionContext();
        transactionContext.beginTransaction();
        return transactionContext;
    }

    private void populateMap(TransactionalMap<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }
    }
}
