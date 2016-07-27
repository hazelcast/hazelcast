package com.hazelcast.client.map;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class AbstractMapPartitionIteratorTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean prefetchValues;

    @Parameterized.Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    protected TestHazelcastFactory factory;
    protected HazelcastInstance server;
    protected HazelcastInstance client;

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() throws Exception {
        ClientMapProxy<Integer, Integer> map = getMapProxy();

        Iterator<Map.Entry<Integer, Integer>> iterator = map.iterator(10, 1, prefetchValues);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = map.iterator(10, 1, prefetchValues);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        map.put(key, value);

        Iterator<Map.Entry<String, String>> iterator = map.iterator(10, 1, prefetchValues);
        Map.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() throws Exception {
        ClientMapProxy<String, String> map = getMapProxy();
        String value = randomString();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(server, 42);
            map.put(key, value);
        }
        Iterator<Map.Entry<String, String>> iterator = map.iterator(10, 42, prefetchValues);
        for (int i = 0; i < count; i++) {
            Map.Entry entry = iterator.next();
            assertEquals(value, entry.getValue());
        }
    }

    private <K, V> ClientMapProxy<K, V> getMapProxy() {
        String mapName = randomString();
        return (ClientMapProxy<K, V>) client.getMap(mapName);
    }
}
