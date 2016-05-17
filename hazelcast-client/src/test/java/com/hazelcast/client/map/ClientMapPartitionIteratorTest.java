package com.hazelcast.client.map;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapPartitionIteratorTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean prefetchValues;

    @Parameterized.Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    private TestHazelcastFactory factory;
    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void init() {
        Config config = getConfig();
        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    private <K, V> ClientMapProxy<K, V> getMapProxy() {
        String mapName = randomString();
        return (ClientMapProxy<K, V>) client.getMap(mapName);
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
}
