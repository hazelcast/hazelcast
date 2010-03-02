/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.impl.SleepCallable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicClusterTest {
    HazelcastClient client;

    @Before
    public void before() {
    }

    @After
    public void after() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
        Hazelcast.shutdownAll();
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
            System.out.println("Counter: " + counter);
            memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
            counter++;
        }
        System.out.println("finish");
    }

    @Test(expected = RuntimeException.class)
    public void throwsRuntimeExceptionWhenNoMemberToConnect() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        Map realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = getHazelcastClient(h1, h2, h3);
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
        map.addEntryListener(listener2, "hello", true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        map.removeEntryListener(listener2, "hello");
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
        client = getHazelcastClient(h1, h2);
        final ITopic<String> topic = client.getTopic("ABC");
        final CountDownLatch latch = new CountDownLatch(2);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                if (msg.equals(message)) {
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
        for (int i = 0; i < 2; i++) {
            new Thread(new Runnable() {

                public void run() {
                    System.out.println("Thread number " + Thread.currentThread().getId());
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
        Lock lock = getHazelcastClient(h).getLock("testGetInstancesCreatedFromClient");
        Collection<Instance> caches = getHazelcastClient(h).getInstances();
        assertEquals(0, caches.size());
        List listOfInstances = new ArrayList();
        listOfInstances.add(list);
        listOfInstances.add(map);
        listOfInstances.add(mmap);
        listOfInstances.add(q);
        listOfInstances.add(set);
        listOfInstances.add(topic);
        listOfInstances.add(lock);
        list.add("List");
        map.put("key", "value");
        assertEquals(2, getHazelcastClient(h).getInstances().size());
        mmap.put("key", "value1");
        q.offer("Element");
        assertEquals(4, getHazelcastClient(h).getInstances().size());
        set.add("element");
        topic.publish("Message");
//        assertEquals(7, getHazelcastClient(h).getInstances().size());
        caches = getHazelcastClient(h).getInstances();
        for (Iterator<Instance> instanceIterator = caches.iterator(); instanceIterator.hasNext();) {
            Instance instance = instanceIterator.next();
            System.out.println("INstance id:" + instance.getId().toString());
            assertTrue(instance.getId().toString().endsWith("testGetInstancesCreatedFromClient"));
            assertTrue(listOfInstances.contains(instance));
            instance.destroy();
        }
        h.shutdown();
    }

    //ok to fail due to lock
    @Test
    public void testGetInstancesCreatedFromCluster() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        List list = h.getList("testGetInstancesCreatedFromCluster");
        Map map = h.getMap("testGetInstancesCreatedFromCluster");
        MultiMap mmap = h.getMultiMap("testGetInstancesCreatedFromCluster");
        Queue q = h.getQueue("testGetInstancesCreatedFromCluster");
        Set set = h.getSet("testGetInstancesCreatedFromCluster");
        ITopic topic = h.getTopic("testGetInstancesCreatedFromCluster");
        ILock lock = h.getLock("testGetInstancesCreatedFromCluster");
        List listOfInstances = new ArrayList();
        listOfInstances.add(list);
        listOfInstances.add(map);
        listOfInstances.add(mmap);
        listOfInstances.add(q);
        listOfInstances.add(set);
        listOfInstances.add(topic);
        listOfInstances.add(lock);
        Collection<Instance> caches = getHazelcastClient(h).getInstances();
        assertEquals(listOfInstances.size(), caches.size());
        for (Iterator<Instance> instanceIterator = caches.iterator(); instanceIterator.hasNext();) {
            Instance instance = instanceIterator.next();
            System.out.println("INstance id:" + instance.getId().toString());
            assertTrue(instance.getId().toString().endsWith("testGetInstancesCreatedFromCluster"));
            assertTrue(listOfInstances.contains(instance));
        }
        h.shutdown();
    }

    @Test
    public void testAuthenticate() {
        String grName = "dev";
        String grPass = "pass";
        Config conf = new Config();
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        HazelcastClient client = HazelcastClient.newHazelcastClient(grName, grPass, true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
    }

    @Test
    public void testAuthenticateWithEmptyPass() {
        String grName = "dev";
        String grPass = "";
        Config conf = new Config();
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        HazelcastClient client = HazelcastClient.newHazelcastClient(grName, "", true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
    }

    @Test(expected = RuntimeException.class)
    public void testAuthenticateWrongPass() {
        String grName = "dev";
        String grPass = "pass";
        Config conf = new Config();
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        HazelcastClient client = HazelcastClient.newHazelcastClient(grName, "wrong-pass", true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
    }

    @Test
    public void addMemberShipListener() throws InterruptedException {
        String grName = "dev";
        String grPass = "pass";
        Config conf = new Config();
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        HazelcastClient client = HazelcastClient.newHazelcastClient(grName, grPass, true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        final CountDownLatch added = new CountDownLatch(1);
        final CountDownLatch removed = new CountDownLatch(1);
        final Map<String, Member> map = new HashMap<String, Member>();
        client.getCluster().addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                added.countDown();
                map.put("Added", membershipEvent.getMember());
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                removed.countDown();
                map.put("Removed", membershipEvent.getMember());
            }
        });
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(conf);
        Member member = h2.getCluster().getLocalMember();
        h2.shutdown();
        assertTrue(added.await(10, TimeUnit.SECONDS));
        assertEquals(member.getInetSocketAddress(), map.get("Added").getInetSocketAddress());
        assertTrue(removed.await(10, TimeUnit.SECONDS));
        assertEquals(member.getInetSocketAddress(), map.get("Removed").getInetSocketAddress());
    }

    @Test
    public void retrieveDataSerializableClass() throws InterruptedException {
        Config conf = new Config();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        HazelcastClient client = HazelcastClient.newHazelcastClient(conf.getGroupConfig().getName(), conf.getGroupConfig().getPassword(), false, h.getCluster().getLocalMember().getInetSocketAddress());
        IMap<Integer, DataSerializableUser> clientMap = client.getMap("retreiveDataSerializableClass");
        final DataSerializableUser user = new DataSerializableUser();
        user.setName("name");
        user.setFamilyName("fName");
        user.setAge(30);
        user.setAddress(new DataSerializableUser.Address());
        final CountDownLatch cdl = new CountDownLatch(2);
        EntryListener<Integer, DataSerializableUser> listener = new EntryListener<Integer, DataSerializableUser>() {
            public void entryAdded(EntryEvent<Integer, DataSerializableUser> entryEvent) {
                DataSerializableUser u = entryEvent.getValue();
                assertEquals(user.getName(), u.getName());
                assertEquals(user.getFamilyName(), u.getFamilyName());
                assertEquals(user.getAge(), u.getAge());
                assertEquals(user.getAddress().getAddress(), u.getAddress().getAddress());
                cdl.countDown();
            }

            public void entryRemoved(EntryEvent entryEvent) {
            }

            public void entryUpdated(EntryEvent entryEvent) {
            }

            public void entryEvicted(EntryEvent entryEvent) {
            }
        };
        clientMap.addEntryListener(listener, true);
        IMap<Integer, DataSerializableUser> clusterMap = h.getMap("retreiveDataSerializableClass");
        clusterMap.addEntryListener(listener, true);
        clientMap.put(1, user);
        DataSerializableUser dsu = clientMap.get(1);
        assertEquals(user.getName(), dsu.getName());
        assertEquals(user.getFamilyName(), dsu.getFamilyName());
        assertEquals(user.getAge(), dsu.getAge());
        assertEquals(user.getAddress().getAddress(), dsu.getAddress().getAddress());
        assertTrue(cdl.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void clientWithAutoMemberListUpdate() throws InterruptedException {
        Config conf = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(conf);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(conf.getGroupConfig().getName(), conf.getGroupConfig().getPassword(),
                h1.getCluster().getLocalMember().getInetSocketAddress().toString().substring(1));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(conf);
        Thread.sleep(1000);
        h1.shutdown();
        Map<Integer, Integer> map = client.getMap("map");
        map.put(1, 1);
        assertEquals(Integer.valueOf(1), map.get(1));
    }

    @Test
    public void ensureClientWillUpdateMembersList() throws InterruptedException {
        Config conf = new Config();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(conf);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(conf.getGroupConfig().getName(), conf.getGroupConfig().getPassword(),
                h1.getCluster().getLocalMember().getInetSocketAddress().toString().substring(1));
        Thread.sleep(1000);
        h1.shutdown();
        Map<Integer, Integer> map = client.getMap("map");
        map.put(1, 1);
        assertEquals(Integer.valueOf(1), map.get(1));
    }

    @Test(timeout = 25000, expected = MemberLeftException.class)
    public void shouldThrowMemberLeftExcWhenNotConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = getHazelcastClient(h2);
        Set<Member> members = client.getCluster().getMembers();
        MultiTask<Long> task =
                new MultiTask<Long>(new SleepCallable(10000), members);
        client.getExecutorService().submit(task);
        Thread.sleep(2000);
        h1.shutdown();
        task.get();
    }

    @Test(timeout = 25000, expected = ExecutionException.class)
    public void shouldThrowExExcptnWhenTheOnlyConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = getHazelcastClient(h2);
        Set<Member> members = client.getCluster().getMembers();
        MultiTask<Long> task =
                new MultiTask<Long>(new SleepCallable(10000), members);
        client.getExecutorService().submit(task);
        Thread.sleep(2000);
        h2.shutdown();
        task.get();
    }

    @Test(timeout = 25000, expected = MemberLeftException.class)
    public void shouldThrowMemberLeftExceptionWhenConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = getHazelcastClient(h1, h2);
        Set<Member> members = client.getCluster().getMembers();
        MultiTask<Long> task =
                new MultiTask<Long>(new SleepCallable(10000), members);
        client.getExecutorService().submit(task);
        Thread.sleep(2000);
        int port = client.getConnectionManager().getConnection().getAddress().getPort();
        if (h1.getCluster().getLocalMember().getInetSocketAddress().getPort() == port) {
            h1.shutdown();
        } else {
            h2.shutdown();
        }
        task.get();
    }

    @Test
    public void shouldThrowExecExcWhenConnectedClusterMemberDies() throws ExecutionException, InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = getHazelcastClient(h);
        Future future1 = client.getExecutorService().submit(new SleepCallable(10000));
        Future future2 = client.getExecutorService().submit(new SleepCallable(10000));
        Thread.sleep(2000);
        h.shutdown();
        CountDownLatch latch = new CountDownLatch(2);
        try {
            future1.get();
        } catch (ExecutionException e) {
            latch.countDown();
        }
        try {
            future2.get();
        } catch (ExecutionException e) {
            latch.countDown();
        }
        latch.await(10, TimeUnit.SECONDS);
    }

    @Test(expected = NoMemberAvailableException.class)
    public void getClusterInFailOver() throws InterruptedException {
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        final HazelcastClient client = getHazelcastClient(h);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                h.shutdown();
            }
        }).start();
        while (true) {
            Set<Member> members = client.getCluster().getMembers();
            System.out.println("Members Size: " + members.size());
        }
    }

    private Map<Integer, HazelcastInstance> getMapOfClusterMembers(HazelcastInstance... h) {
        Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
        for (HazelcastInstance hazelcastInstance : h) {
            memberMap.put(hazelcastInstance.getCluster().getLocalMember().getPort(), hazelcastInstance);
        }
        return memberMap;
    }

    @Test
    public void twoClientsAndTransaction() {
        Config config1 = new XmlConfigBuilder().build();
        Config config2 = new XmlConfigBuilder().build();
        GroupConfig gConfig1 = new GroupConfig("g1", "pg1");
        GroupConfig gConfig2 = new GroupConfig("g2", "pg2");
        config1.setGroupConfig(gConfig1);
        config2.setGroupConfig(gConfig2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        HazelcastInstance client1 = getHazelcastClient(h1);
        HazelcastInstance client2 = getHazelcastClient(h2);
        Transaction t1 = client1.getTransaction();
        Transaction t2 = client2.getTransaction();
        t1.begin();
        client1.getMap("map").put(1,4);
    }
}
