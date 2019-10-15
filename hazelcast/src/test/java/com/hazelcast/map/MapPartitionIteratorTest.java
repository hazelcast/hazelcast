/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPartitionIteratorTest extends HazelcastTestSupport {

    @Parameter
    public boolean prefetchValues;

    @Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    @Test(expected = NoSuchElementException.class)
    public void test_next_Throws_Exception_On_EmptyPartition() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        iterator.next();
    }

    @Test(expected = IllegalStateException.class)
    public void test_remove_Throws_Exception_When_Called_Without_Next() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        iterator.remove();
    }

    @Test
    public void test_Remove() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        iterator.next();
        iterator.remove();
        assertEquals(0, proxy.size());
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        Map.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition_and_HasNext_Returns_False_when_Item_Consumed() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        proxy.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        Map.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<Object, Object> proxy = (MapProxyImpl<Object, Object>) instance.getMap(randomMapName());

        String value = randomString();
        for (int i = 0; i < 100; i++) {
            String key = generateKeyForPartition(instance, 1);
            proxy.put(key, value);
        }
        Iterator<Map.Entry<Object, Object>> iterator = proxy.iterator(10, 1, prefetchValues);
        for (int i = 0; i < 100; i++) {
            Map.Entry entry = iterator.next();
            assertEquals(value, entry.getValue());
        }
    }

    @Test
    @Ignore
    public void test_DoesNotReturn_DuplicateEntry_When_Rehashing_Happens() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        MapProxyImpl<String, String> proxy = (MapProxyImpl<String, String>) instance.<String, String>getMap(randomMapName());
        HashSet<String> readKeys = new HashSet<String>();

        String value = "initialValue";
        putValuesToPartition(instance, proxy, value, 1, 100);
        Iterator<Map.Entry<String, String>> iterator = proxy.iterator(10, 1, prefetchValues);
        assertUniques(readKeys, iterator, 50);
        // force rehashing
        putValuesToPartition(instance, proxy, randomString(), 1, 150);
        assertUniques(readKeys, iterator);
    }

    @Test
    @Ignore
    public void test_DoesNotReturn_DuplicateEntry_When_Migration_Happens() throws Exception {
        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "2");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        MapProxyImpl<String, String> proxy = (MapProxyImpl<String, String>) instance.<String, String>getMap(randomMapName());

        HashSet<String> readKeysP1 = new HashSet<String>();
        HashSet<String> readKeysP2 = new HashSet<String>();

        String value = "value";
        putValuesToPartition(instance, proxy, value, 0, 100);
        putValuesToPartition(instance, proxy, value, 1, 100);

        Iterator<Map.Entry<String, String>> iteratorP1 = proxy.iterator(10, 0, prefetchValues);
        Iterator<Map.Entry<String, String>> iteratorP2 = proxy.iterator(10, 1, prefetchValues);
        assertUniques(readKeysP1, iteratorP1, 50);
        assertUniques(readKeysP2, iteratorP2, 50);
        // force migration
        factory.newHazelcastInstance(config);
        // force rehashing
        putValuesToPartition(instance, proxy, randomString(), 0, 150);
        putValuesToPartition(instance, proxy, randomString(), 1, 150);
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

    private void assertUniques(HashSet<String> readKeys, Iterator<Map.Entry<String, String>> iterator, int numberOfItemsToRead) {
        int count = 0;
        while (iterator.hasNext() && count++ < numberOfItemsToRead) {
            Map.Entry<String, String> entry = iterator.next();
            boolean unique = readKeys.add(entry.getKey());
            Assert.assertTrue(unique);
        }
    }

    private void putValuesToPartition(HazelcastInstance instance, MapProxyImpl<String, String> proxy, String value,
                                      int partitionId, int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instance, partitionId);
            proxy.put(key, value);
        }
    }
}
