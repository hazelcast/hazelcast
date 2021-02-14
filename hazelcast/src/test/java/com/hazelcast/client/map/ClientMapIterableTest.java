/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapIterableTest extends HazelcastTestSupport {

    @Parameter
    public boolean prefetchValues;

    @Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[] {Boolean.TRUE}, new Object[] {Boolean.FALSE});
    }

    protected TestHazelcastFactory factory;
    protected HazelcastInstance server;
    protected HazelcastInstance client;

    @Before
    public void setup() {
        Config config = getConfig();
        ClientConfig clientConfig = getClientConfig();

        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient(clientConfig);
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test(expected = NoSuchElementException.class)
    public void test_next_Throws_Exception_On_EmptyMap() {
        ClientMapProxy<Object, Object> proxy = (ClientMapProxy<Object, Object>) client.getMap(randomMapName());

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterable(10, prefetchValues).iterator();
        iterator.next();
    }

    @Test(expected = IllegalStateException.class)
    public void test_remove_Throws_Exception_When_Called_Without_Next() {
        ClientMapProxy<Object, Object> proxy = (ClientMapProxy<Object, Object>) client.getMap(randomMapName());

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterable(10, prefetchValues).iterator();
        iterator.remove();
    }

    @Test
    public void test_Remove() {
        ClientMapProxy<Object, Object> proxy = (ClientMapProxy<Object, Object>) client.getMap(randomMapName());

        String key = generateKeyForPartition(client, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterable(10, prefetchValues).iterator();
        iterator.next();
        iterator.remove();
        assertEquals(0, proxy.size());
    }

    @Test
    public void test_remove_withMultiplePartitions() {
        String mapName = randomMapName();
        ClientMapProxy<Object, Object> proxy = (ClientMapProxy<Object, Object>) client.getMap(mapName);

        String key = generateKeyForPartition(client, 1);
        String value = randomString();
        proxy.put(key, value);

        String key2 = generateKeyForPartition(client, 2);
        String value2 = randomString();
        proxy.put(key2, value2);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterable(10, prefetchValues).iterator();
        iterator.next();
        iterator.remove();
        assertEquals(1, proxy.size());
        iterator.next();
        iterator.remove();
        assertEquals(0, proxy.size());
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyMap() {
        ClientMapProxy<Object, Object> proxy = (ClientMapProxy<Object, Object>) client.getMap(randomMapName());

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterable(10, prefetchValues).iterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyMap() {
        ClientMapProxy<Object, Object> proxy = (ClientMapProxy<Object, Object>) client.getMap(randomMapName());

        String key = generateKeyForPartition(client, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterable(10, prefetchValues).iterator();
        Assert.assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyMap() {
        ClientMapProxy<String, String> proxy = (ClientMapProxy<String, String>) client.<String, String>getMap(randomMapName());

        String key = generateKeyForPartition(client, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = proxy.iterable(10, prefetchValues).iterator();
        Map.Entry<String, String> entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyMap_and_HasNext_Returns_False_when_Item_Consumed() {
        ClientMapProxy<String, String> proxy = (ClientMapProxy<String, String>) client.<String, String>getMap(randomMapName());

        String key = generateKeyForPartition(client, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = proxy.iterable(10, prefetchValues).iterator();
        Map.Entry<String, String> entry = iterator.next();
        assertEquals(value, entry.getValue());
        boolean hasNext = iterator.hasNext();
        Assert.assertFalse(hasNext);
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyMap() {
        ClientMapProxy<String, String> proxy = (ClientMapProxy<String, String>) client.<String, String>getMap(randomMapName());

        String value = randomString();
        for (int i = 0; i < 100; i++) {
            String key = generateKeyForPartition(client, 1);
            proxy.put(key, value);
        }
        Iterator<Map.Entry<String, String>> iterator = proxy.iterable(10, prefetchValues).iterator();
        for (int i = 0; i < 100; i++) {
            Map.Entry<String, String> entry = iterator.next();
            assertEquals(value, entry.getValue());
        }
    }

    @Test
    public void test_DoesNotReturn_DuplicateEntry_When_Rehashing_Happens() {
        ClientMapProxy<String, String> proxy = (ClientMapProxy<String, String>) client.<String, String>getMap(randomMapName());
        HashSet<String> readKeys = new HashSet<>();

        String value = "initialValue";
        putValuesToPartition(client, proxy, value, 1, 100);
        Iterator<Map.Entry<String, String>> iterator = proxy.iterable(10, prefetchValues).iterator();
        assertUniques(readKeys, iterator, 50);
        // force rehashing
        putValuesToPartition(client, proxy, randomString(), 1, 150);
        assertUniques(readKeys, iterator);
    }

    @Test
    public void test_DoesNotReturn_DuplicateEntry_When_Migration_Happens() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance client = factory.newHazelcastInstance(config);
        ClientMapProxy<String, String> proxy =
                (ClientMapProxy<String, String>) ClientMapIterableTest.this.client.<String, String>getMap(randomMapName());

        HashSet<String> readKeysP1 = new HashSet<>();
        HashSet<String> readKeysP2 = new HashSet<>();

        String value = "value";
        putValuesToPartition(ClientMapIterableTest.this.client, proxy, value, 0, 100);
        putValuesToPartition(ClientMapIterableTest.this.client, proxy, value, 1, 100);

        Iterator<Map.Entry<String, String>> iteratorP1 = proxy.iterable(10, prefetchValues).iterator();
        Iterator<Map.Entry<String, String>> iteratorP2 = proxy.iterable(10, prefetchValues).iterator();
        assertUniques(readKeysP1, iteratorP1, 50);
        assertUniques(readKeysP2, iteratorP2, 50);
        // force migration
        factory.newHazelcastInstance(config);
        // force rehashing
        putValuesToPartition(ClientMapIterableTest.this.client, proxy, randomString(), 0, 150);
        putValuesToPartition(ClientMapIterableTest.this.client, proxy, randomString(), 1, 150);
        assertUniques(readKeysP1, iteratorP1);
        assertUniques(readKeysP2, iteratorP2);
    }

    private void assertUniques(HashSet<String> readKeys, Iterator<Map.Entry<String, String>> iterator) {
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            boolean unique = readKeys.add(entry.getKey());
            Assert.assertTrue(unique);
        }
    }

    private void assertUniques(HashSet<String> readKeys,
                               Iterator<Map.Entry<String, String>> iterator,
                               int numberOfItemsToRead) {
        int count = 0;
        while (iterator.hasNext() && count++ < numberOfItemsToRead) {
            Map.Entry<String, String> entry = iterator.next();
            boolean unique = readKeys.add(entry.getKey());
            Assert.assertTrue(unique);
        }
    }

    private void putValuesToPartition(HazelcastInstance instance, ClientMapProxy<String, String> proxy, String value,
                                      int partitionId, int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instance, partitionId);
            proxy.put(key, value);
        }
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> ClientMapProxy<K, V> getMapProxy() {
        String mapName = randomString();
        return (ClientMapProxy<K, V>) client.getMap(mapName);
    }
}
