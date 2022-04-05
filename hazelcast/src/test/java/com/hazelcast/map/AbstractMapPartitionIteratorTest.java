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

package com.hazelcast.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class AbstractMapPartitionIteratorTest extends HazelcastTestSupport {

    protected TestHazelcastFactory factory;
    protected HazelcastInstance instance;

    @Parameter
    public boolean prefetchValues;

    @Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[] {Boolean.TRUE}, new Object[] {Boolean.FALSE});
    }

    @Before
    public abstract void setup();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    protected abstract <K, V> Iterator<Map.Entry<K, V>> getIterator(IMap<K, V> map, int partitionId);

    protected Config getConfig() {
        // Partition count must be two in order to  properly run this test:
        // test_DoesNotReturn_DuplicateEntry_When_Migration_Happens
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");
        return config;
    }

    protected <K, V> Iterator<Map.Entry<K, V>> getIterator(IMap<K, V> map) {
        return getIterator(map, 1);
    }

    @Test(expected = NoSuchElementException.class)
    public void test_next_Throws_Exception_On_EmptyPartition() {
        IMap<Object, Object> map = instance.getMap(randomMapName());
        Iterator<Map.Entry<Object, Object>> iterator = getIterator(map);
        iterator.next();
    }

    @Test(expected = IllegalStateException.class)
    public void test_remove_Throws_Exception_When_Called_Without_Next() {
        IMap<Object, Object> map = instance.getMap(randomMapName());
        Iterator<Map.Entry<Object, Object>> iterator = getIterator(map);
        iterator.remove();
    }

    @Test
    public void test_Remove() {
        IMap<Object, Object> map = instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = getIterator(map);
        iterator.next();
        iterator.remove();
        assertEquals(0, map.size());
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() {
        IMap<Object, Object> map = instance.getMap(randomMapName());
        Iterator<Map.Entry<Object, Object>> iterator = getIterator(map);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() {
        IMap<Object, Object> map = instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<Object, Object>> iterator = getIterator(map);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() {
        IMap<String, String> map = instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = getIterator(map);
        Map.Entry<String, String> entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition_and_HasNext_Returns_False_when_Item_Consumed() {
        IMap<String, String> map = instance.getMap(randomMapName());

        String key = generateKeyForPartition(instance, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = getIterator(map);
        Map.Entry<String, String> entry = iterator.next();
        assertEquals(value, entry.getValue());
        boolean hasNext = iterator.hasNext();
        assertFalse(hasNext);
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() {
        IMap<String, String> map = instance.getMap(randomMapName());

        String value = randomString();
        putValuesToPartition(instance, map, value, 1, 100);
        Iterator<Map.Entry<String, String>> iterator = getIterator(map);
        for (int i = 0; i < 100; i++) {
            Map.Entry<String, String> entry = iterator.next();
            assertEquals(value, entry.getValue());
        }
    }

    @Test
    public void test_DoesNotReturn_DuplicateEntry_When_Rehashing_Happens() {
        IMap<String, String> map = instance.getMap(randomMapName());
        HashSet<String> readKeys = new HashSet<>();

        String value = "initialValue";
        putValuesToPartition(instance, map, value, 1, 100);
        Iterator<Map.Entry<String, String>> iterator = getIterator(map);
        assertUniques(readKeys, iterator, 50);
        // force rehashing
        putValuesToPartition(instance, map, randomString(), 1, 150);
        assertUniques(readKeys, iterator);
    }

    @Test
    public void test_DoesNotReturn_DuplicateEntry_When_Migration_Happens() {
        IMap<String, String> map = instance.getMap(randomMapName());

        HashSet<String> readKeysP1 = new HashSet<>();
        HashSet<String> readKeysP2 = new HashSet<>();

        String value = "value";
        putValuesToPartition(instance, map, value, 0, 100);
        putValuesToPartition(instance, map, value, 1, 100);

        Iterator<Map.Entry<String, String>> iteratorP1 = getIterator(map, 0);
        Iterator<Map.Entry<String, String>> iteratorP2 = getIterator(map, 1);
        assertUniques(readKeysP1, iteratorP1, 50);
        assertUniques(readKeysP2, iteratorP2, 50);
        // force migration
        factory.newHazelcastInstance(getConfig());
        // force rehashing
        putValuesToPartition(instance, map, randomString(), 0, 150);
        putValuesToPartition(instance, map, randomString(), 1, 150);
        assertUniques(readKeysP1, iteratorP1);
        assertUniques(readKeysP2, iteratorP2);
    }

    private void assertUniques(HashSet<String> readKeys, Iterator<Map.Entry<String, String>> iterator) {
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            boolean unique = readKeys.add(entry.getKey());
            assertTrue(unique);
        }
    }

    private void assertUniques(HashSet<String> readKeys,
                               Iterator<Map.Entry<String, String>> iterator,
                               int numberOfItemsToRead) {
        int count = 0;
        while (iterator.hasNext() && count++ < numberOfItemsToRead) {
            Map.Entry<String, String> entry = iterator.next();
            boolean unique = readKeys.add(entry.getKey());
            assertTrue(unique);
        }
    }

    private void putValuesToPartition(HazelcastInstance instance, IMap<String, String> proxy, String value,
                                      int partitionId, int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(instance, partitionId);
            proxy.put(key, value);
        }
    }
}
