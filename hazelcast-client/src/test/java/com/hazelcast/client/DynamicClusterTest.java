package com.hazelcast.client;

import static com.hazelcast.client.TestUtility.*;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import com.hazelcast.core.*;

public class DynamicClusterTest {
    HazelcastClient client;

    @Before
    public void before() {
    }

    @After
    public void after() throws InterruptedException {
        Hazelcast.shutdownAll();
        if(client!=null){
            client.shutdown();
        }
//        Thread.sleep(500);
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
            map.put("currentIteratedKey", counter);
            assertEquals(counter, realMap.get("currentIteratedKey"));
            assertEquals(counter, map.get("currentIteratedKey"));
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
        realMap.get("currentIteratedKey");
        while (counter < 3) {
            map.put("currentIteratedKey", counter);
            assertEquals(counter, map.get("currentIteratedKey"));
            assertEquals(counter, realMap.get("currentIteratedKey"));
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
        System.out.println("Map is putting");
        map.put("hello", "world");
        System.out.println("map put");
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


        topic.addMessageListener(new MessageListener<String>()
        {
            public void onMessage(String msg) {
                if(msg.equals(message)){
                    latch.countDown();
                }
                System.out.println(msg);
            }
        });

        topic.publish(message);

        HazelcastInstance h = memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort());
        h.shutdown();
        System.out.println("Shut down: " + h.getCluster().getLocalMember().getPort());
        Thread.sleep(1000);
        for(int i=0;i<2;i++){

            new Thread(new Runnable(){

                public void run() {
                    System.out.println("Thread number "+ Thread.currentThread().getId() );
                    topic.publish(message);
                }
            }).start();
        }

        Thread.sleep(1000);
        System.out.println("FINISHED");
        assertTrue(latch.await(2, TimeUnit.SECONDS));

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

     @Test
    public void testGetInstancesCreatedFromClient() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        List list = getHazelcastClient(h).getList("testGetInstancesCreatedFromClient");
        Map map = getHazelcastClient(h).getMap("testGetInstancesCreatedFromClient");
        MultiMap mmap = getHazelcastClient(h).getMultiMap("testGetInstancesCreatedFromClient");
        Queue q = getHazelcastClient(h).getQueue("testGetInstancesCreatedFromClient");
        Set set = getHazelcastClient(h).getSet("testGetInstancesCreatedFromClient");
        ITopic topic = getHazelcastClient(h).getTopic("testGetInstancesCreatedFromClient");

        Collection<Instance> caches = getHazelcastClient(h).getInstances();
        assertEquals(0, caches.size());
        List listOfInstances = new ArrayList();
        listOfInstances.add(list);
        listOfInstances.add(map);
        listOfInstances.add(mmap);
        listOfInstances.add(q);
        listOfInstances.add(set);
        listOfInstances.add(topic);
        list.add("List");
        map.put("key", "value");
        assertEquals(2, getHazelcastClient(h).getInstances().size());
        mmap.put("key", "value1");
        q.offer("Element");
        assertEquals(4, getHazelcastClient(h).getInstances().size());
        set.add("element");
        topic.publish("Message");
        assertEquals(6, getHazelcastClient(h).getInstances().size());
        caches = getHazelcastClient(h).getInstances();
        for (Iterator<Instance> instanceIterator = caches.iterator(); instanceIterator.hasNext();) {
            Instance instance = instanceIterator.next();
            assertTrue(instance.getId().toString().endsWith("testGetInstancesCreatedFromClient"));
            assertTrue(listOfInstances.contains(instance));
            instance.destroy();
        }
        h.shutdown();
    }
    @Test
    public void testGetInstancesCreatedFromCluster() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        List list = h.getList("testGetInstancesCreatedFromCluster");
        Map map = h.getMap("testGetInstancesCreatedFromCluster");
        MultiMap mmap = h.getMultiMap("testGetInstancesCreatedFromCluster");
        Queue q = h.getQueue("testGetInstancesCreatedFromCluster");
        Set set = h.getSet("testGetInstancesCreatedFromCluster");
        ITopic topic = h.getTopic("testGetInstancesCreatedFromCluster");

        List listOfInstances = new ArrayList();
        listOfInstances.add(list);
        listOfInstances.add(map);
        listOfInstances.add(mmap);
        listOfInstances.add(q);
        listOfInstances.add(set);
        listOfInstances.add(topic);

        Collection<Instance> caches = getHazelcastClient(h).getInstances();
//        assertEquals(6, caches.size());
        for (Iterator<Instance> instanceIterator = caches.iterator(); instanceIterator.hasNext();) {
            Instance instance = instanceIterator.next();
            assertTrue(instance.getId().toString().endsWith("testGetInstancesCreatedFromCluster"));
            assertTrue(listOfInstances.contains(instance));
        }
        h.shutdown();
    }

     @Test
    public void testAuthenticate(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = HazelcastClient.newHazelcastClient("dev", "dev-pass", true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
    }

    @Test(expected = RuntimeException.class)
    public void testAuthenticateWrongPass(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = HazelcastClient.newHazelcastClient("dev", "wrong-pass", true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
    }

    private Map<Integer, HazelcastInstance> getMapOfClusterMembers(HazelcastInstance... h) {
        Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
        for (HazelcastInstance hazelcastInstance : h) {
            memberMap.put(hazelcastInstance.getCluster().getLocalMember().getPort(), hazelcastInstance);
        }
        return memberMap;
    }
}
