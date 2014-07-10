package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.helpers.PortableHelpersFactory;
import com.hazelcast.client.helpers.SimpleClientInterceptor;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * User: danny Date: 11/26/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleClientMapInterceptorTest {

    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance client;

    static SimpleClientInterceptor interceptor;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        server1 = Hazelcast.newHazelcastInstance(config);
        server2 = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        client = HazelcastClient.newHazelcastClient(clientConfig);

        interceptor = new SimpleClientInterceptor();
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void clientMapInterceptorTestIssue1238() throws InterruptedException {
        IMap<Object, Object> map = client.getMap("clientMapInterceptorTest");

        String id = map.addInterceptor(interceptor);

        map.put(1, "New York");
        map.put(2, "Istanbul");
        map.put(3, "Tokyo");
        map.put(4, "London");
        map.put(5, "Paris");
        map.put(6, "Cairo");
        map.put(7, "Hong Kong");

        map.remove(1);
        try {
            map.remove(2);
            fail();
        } catch (Exception ignore) {
        }

        assertEquals(map.size(), 6);
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL:");
        assertEquals(map.get(3), "TOKYO:");
        assertEquals(map.get(4), "LONDON:");
        assertEquals(map.get(5), "PARIS:");
        assertEquals(map.get(6), "CAIRO:");
        assertEquals(map.get(7), "HONG KONG:");

        map.removeInterceptor(id);
        map.put(8, "Moscow");

        assertEquals(map.get(8), "Moscow");
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL");
        assertEquals(map.get(3), "TOKYO");
        assertEquals(map.get(4), "LONDON");
        assertEquals(map.get(5), "PARIS");
        assertEquals(map.get(6), "CAIRO");
        assertEquals(map.get(7), "HONG KONG");
    }
}