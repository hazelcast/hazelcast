package com.hazelcast.client.starter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class HazelcastClientStarterTest {

    @Test
    public void testClientLifecycle() throws InterruptedException {
        HazelcastInstance member = HazelcastStarter.newHazelcastInstance("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting client " + version);
            HazelcastInstance instance = HazelcastClientStarter.newHazelcastClient(version, false);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }

        member.shutdown();
    }

    @Test
    public void testClientMap() throws InterruptedException {
        HazelcastInstance memberInstance = HazelcastStarter.newHazelcastInstance("3.7");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("3.7.2", false);

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        IMap<Integer, Integer> memberMap = memberInstance.getMap("myMap");

        clientMap.put(1, 2);

        assertEquals(2, (int) memberMap.get(1));

        clientInstance.shutdown();
        memberInstance.shutdown();
    }

    @Test
    public void testAdvancedClientMap() throws InterruptedException {
        HazelcastInstance memberInstance = HazelcastStarter.newHazelcastInstance("3.7");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("3.7.2", false);

        System.out.println("About to terminate the client");
        clientInstance.getLifecycleService().terminate();
        System.out.println("Client terminated");

        memberInstance.shutdown();
    }

    @Test
    public void testClientMap_async() throws InterruptedException, ExecutionException {
        HazelcastInstance memberInstance = HazelcastStarter.newHazelcastInstance("3.7");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("3.7.2", false);

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        clientMap.put(0, 1);
        ICompletableFuture<Integer> async = clientMap.getAsync(0);
        int value = async.get();

        assertEquals(1, value);

        clientInstance.shutdown();
        memberInstance.shutdown();
    }
}
