package com.hazelcast.client;

import static com.hazelcast.client.HazelcastClientTest.getHazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;

public class DynamicClusterTest {
    HazelcastClient client;

    @Before
    public void before() {
    }

    @After
    public void after() throws InterruptedException {
        Hazelcast.shutdownAll();
        client.shutdown();
        Thread.sleep(500);
    }

    @Test
    public void continuePutAndGetIfOneOfConnectedClusterMemberFails() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        client = getHazelcastClient(h1, h2);
        Map realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        Map map = client.getMap("default");
        int counter = 0;
        while (counter < 2) {
            map.put("key", counter);
            assertEquals(counter, realMap.get("key"));
            assertEquals(counter, map.get("key"));
            memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
            counter++;
        }
    }

    @Test(expected = RuntimeException.class)
    public void throwsRuntimeExceptionWhenNoMemberToConnect() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        Map realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = getHazelcastClient(h1, h2);
        Map map = client.getMap("default");
        int counter = 0;
        realMap.get("key");
        while (counter < 3) {
            map.put("key", counter);
            assertEquals(counter, map.get("key"));
            assertEquals(counter, realMap.get("key"));
            memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
            counter++;
        }
    }

    @Test
    public void addListenerWithTwoMemberClusterAndKillOne() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        Map<String,String> realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = getHazelcastClient(h1, h2);
        IMap<String, String> map = client.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(2);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(2);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(2);
        map.addEntryListener(new EntryListener<String,String>() {
            public void entryAdded(EntryEvent<String,String> event) {
                assertEquals("hello", event.getKey());
                entryAddLatch.countDown();
            }

            public void entryRemoved(EntryEvent<String,String> event) {
                entryRemovedLatch.countDown();
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getValue());
            }

            public void entryUpdated(EntryEvent<String,String> event) {
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
                entryUpdatedLatch.countDown();
            }

            public void entryEvicted(EntryEvent<String,String> event) {
                entryRemoved(event);
            }
        }, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        realMap.remove("hello");
        memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
        map.put("hello", "world");
        map.put("hello", "new world");
        realMap.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void shutdown() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
            client = getHazelcastClient(h1);
            Map<String, String> clientMap = client.getMap("map1");
            Map<String, String> hMap = h1.getMap("map1");
            clientMap.put("A", String.valueOf(i));
            assertEquals(String.valueOf(i), hMap.get("A"));
            client.shutdown();
            h1.shutdown();
        }
    }

    private Map<Integer, HazelcastInstance> getMapOfClusterMembers(HazelcastInstance... h) {
        Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
        for (HazelcastInstance hazelcastInstance : h) {
            memberMap.put(hazelcastInstance.getCluster().getLocalMember().getPort(), hazelcastInstance);
        }
        return memberMap;
    }
}
