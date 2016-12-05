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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.nearcache.NearCacheTestSupport;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
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
public class ClientMapNearCacheTxTest extends NearCacheTestSupport {

    @Parameterized.Parameter
    public boolean batchInvalidationEnabled;

    @Parameterized.Parameters(name = "batchInvalidationEnabled:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void test_whenEmptyMap_thenPopulatedNearCacheShouldReturnNull_neverNULL_OBJECT() {
        int size = 10;
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = newNearCacheConfig();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = getNearCachedClient(mapName, config, nearCacheConfig);

        // populate map
        TransactionContext transactionContext = client.newTransactionContext();
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
        int mapSize = 5000;
        String mapName = "testBasicUsage";

        NearCacheConfig nearCacheConfig = newNearCacheConfig().setInvalidateOnChange(true);

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = getNearCachedClient(mapName, config, nearCacheConfig);

        TransactionContext transactionContext = client.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMap<Integer, Integer> map = transactionContext.getMap(mapName);
        populateMap(map, mapSize);
        transactionContext.commitTransaction();

        IMap<Integer, Integer> instanceMap = client.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            assertNotNull(instanceMap.get(i));
        }

        transactionContext = client.newTransactionContext();
        transactionContext.beginTransaction();
        map = transactionContext.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i * 2);
        }
        transactionContext.commitTransaction();

        IMap<Object, Object> m = client.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            assertNotNull(m.get(i));
        }

        assertNearCacheNotEmpty(mapName, client);

        transactionContext = client.newTransactionContext();
        transactionContext.beginTransaction();
        map = transactionContext.getMap(mapName);
        for (Integer key : map.keySet()) {
            map.remove(key);
        }
        transactionContext.commitTransaction();
        assertNearCacheEmptyEventually(mapName, client);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), valueOf(batchInvalidationEnabled));
        return config;
    }

    private void populateMap(TransactionalMap<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }
    }

    private ClientConfig newClientConfig() {
        return new ClientConfig();
    }

    private HazelcastInstance getNearCachedClient(String mapName, Config config, NearCacheConfig nearCacheConfig) {
        hazelcastFactory.newHazelcastInstance(config);

        nearCacheConfig.setName(mapName + "*");

        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        return hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Override
    protected NearCache getNearCache(String mapName, HazelcastInstance instance) {
        NearCachedClientMapProxy mapProxy = (NearCachedClientMapProxy) instance.getMap(mapName);

        return mapProxy.getNearCache();
    }
}
