package com.hazelcast.client.spi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientProxyDestroyTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        destroy();
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testUsageAfterDestroy() {
        IAtomicLong proxy = newClientProxy();
        proxy.destroy();
        proxy.get();
    }

    @Test
    public void testMultipleDestroyCalls() {
        IAtomicLong proxy = newClientProxy();
        proxy.destroy();
        proxy.destroy();
    }

    private IAtomicLong newClientProxy() {
        return client.getAtomicLong(HazelcastTestSupport.randomString());
    }

    @Test
    public void testOperationAfterDestroy() throws Exception {
        final String mapName = randomMapName();
        final IMap<Object, Object> clientMap = client.getMap(mapName);
        clientMap.destroy();
        assertFalse(client.getDistributedObjects().contains(clientMap));
        clientMap.put(1, 1);
        assertEquals(1, clientMap.get(1));
    }
}
