package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.executor.DelegatingFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientExecutionPoolSizeLowTest {

    static final int COUNT = 30000;
    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance client;
    static IMap map;

    @Before
    public void init() {
        Config config = new Config();
        server1 = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(1);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        server2 = Hazelcast.newHazelcastInstance(config);

        map = client.getMap(randomString());
    }

    @After
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.put(i, i);
            if (i == COUNT / 2) {
                server2.getLifecycleService().terminate();
            }
        }
        assertEquals(COUNT, map.size());
    }

    @Test
    public void testOwnerNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.put(i, i);
            if (i == COUNT / 2) {
                server1.getLifecycleService().terminate();
            }
        }
        assertEquals(COUNT, map.size());
    }

    @Test
    public void testNodeTerminateWithAsyncOperations() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);
            if (i == COUNT / 2) {
                server2.getLifecycleService().terminate();
            }
        }
        assertSizeEventually(COUNT, map);
    }

    @Test
    public void testOwnerNodeTerminateWithAsyncOperations() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);

            if (i == COUNT / 2) {
                server1.getLifecycleService().terminate();
            }
        }

        assertSizeEventually(COUNT, map);
    }
}
