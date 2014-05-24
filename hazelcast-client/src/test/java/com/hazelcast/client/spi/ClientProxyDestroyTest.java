package com.hazelcast.client.spi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientProxyDestroyTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUsageAfterDestroy() {
        IAtomicLong proxy = newClientProxy();
        proxy.destroy();

        //since the object is destroyed, getting the value from the atomic long should fail
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
}
