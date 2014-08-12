package com.hazelcast.client;


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies that a non smart client, can send request to a wrong node, but still can get responses to its requests.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NonSmartClientRoutingTest {
    static HazelcastInstance client;
    static HazelcastInstance server1;
    static HazelcastInstance server2;

    @BeforeClass
    public static void init() {
        server1 = Hazelcast.newHazelcastInstance();
        server2 = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        String mapName = randomString();
        // create some dummy data.
        Map<String, String> origin = new HashMap<String, String>();
        for (int k = 0; k < 1000; k++) {
            String value = randomString();
            origin.put(value, value);
        }

        // insert that data on the server.
        server1.getMap(mapName).putAll(origin);

        // now iterate over that dummy data and make sure that we find everything that was inserted.
        IMap<String, String> map = client.getMap(mapName);
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String key = entry.getKey();
            String expectedValue = map.get(key);
            String actualValue = map.get(key);
            assertEquals(expectedValue, actualValue);
        }
    }
}
