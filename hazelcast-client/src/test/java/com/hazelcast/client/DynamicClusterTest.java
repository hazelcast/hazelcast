package com.hazelcast.client;

import static com.hazelcast.client.TestUtility.*;
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
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;


import com.hazelcast.core.*;

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
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = getHazelcastClient(h1, h2);
        IMap<String, String> map = client.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(2);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(2);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(2);
        CountDownLatchEntryListener<String, String> listener = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void add2ListenerWithTwoMemberClusterRemoveOneListenerAndKillOneClusterInstance() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = getHazelcastClient(h1, h2);
        IMap<String, String> map = client.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(4);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(4);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(4);
        CountDownLatchEntryListener<String, String> listener = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        CountDownLatchEntryListener<String, String> listener2 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener, true);
        map.addEntryListener(listener2,"hello", true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        map.removeEntryListener(listener2,"hello");
        memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
        assertEquals(1, entryAddLatch.getCount());
        assertEquals(1, entryUpdatedLatch.getCount());
        assertEquals(1, entryRemovedLatch.getCount());
//        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void addMessageListenerWhithClusterFailOver() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = getHazelcastClient(h1,h2);
        final ITopic<String> topic = client.getTopic("ABC");
        final CountDownLatch latch = new CountDownLatch(2);
        final String message =  "Hazelcast Rocks!";


        topic.addMessageListener(new MessageListener()
        {
            public void onMessage(Object msg) {
                if(msg.equals(message)){
                    latch.countDown();
                }
                System.out.println(msg);
            }
        });

        topic.publish(message);

        System.err.println("shut start" + System.currentTimeMillis());
        HazelcastInstance h = memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort());
//        System.out.println(h.getCluster().getLocalMember().getPort());
        h.shutdown();
        System.err.println("shut finished" + System.currentTimeMillis());

        for(int i=0;i<2;i++){
            System.out.println("Thread number"+i);
            new Thread(new Runnable(){

                public void run() {
                    topic.publish(message);
                }
            }).start();
        }

        System.out.println("FINISHED");
        assertTrue(latch.await(10, TimeUnit.MILLISECONDS));

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
