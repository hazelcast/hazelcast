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

package com.hazelcast.client.longrunning;

import com.hazelcast.client.CountDownLatchEntryListener;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.NoMemberAvailableException;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.SleepCallable;
import com.hazelcast.monitor.DistributedMapStatsCallable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.client.HazelcastClientMapTest.getAllThreads;
import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class DynamicClusterTest {
    HazelcastClient client;
    Config config = new Config();

    @Before
    public void before() {
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
    }

    @After
    @Before
    public void after() throws InterruptedException {
        if (client != null) {
            client.shutdown();
            client = null;
        }
        Hazelcast.shutdownAll();
    }

    @Test
    public void continuePutAndGetIfOneOfConnectedClusterMemberFails() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h1, h2);
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
        h3.shutdown();
    }

    @Test
    public void fiveTimesContinuePutAndGetIfOneOfConnectedClusterMemberFails() throws IOException, InterruptedException {
        for (int i = 0; i < 5; i++) {
            continuePutAndGetIfOneOfConnectedClusterMemberFails();
        }
    }

    @Test
    public void test2Instances1ClusterMemberAndFailover() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        final Map<Integer, HazelcastInstance> instanceMap = new ConcurrentHashMap<Integer, HazelcastInstance>();
        new Thread(new Runnable() {
            public void run() {
                HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                instanceMap.put(0, h);
                latch.countDown();
            }
        }).start();
        new Thread(new Runnable() {
            public void run() {
                HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                instanceMap.put(1, h);
                latch.countDown();
            }
        }).start();
        //Thread that will shutdown the second instance
        new Thread(new Runnable() {
            public void run() {
                try {
                    shutdownLatch.await();
                } catch (InterruptedException e) {
                }
                instanceMap.get(1).shutdown();
            }
        }).start();
        latch.await();
        HazelcastClient client = newHazelcastClient(instanceMap.get(1), instanceMap.get(0));
        Map map = client.getMap("myMap");
        int i = 0;
        for (; i < 100; ++i) {
            map.put("test", i);
            if (i == 20) {
                shutdownLatch.countDown();
            }
        }
        assertEquals(i - 1, map.get("test"));
        client.shutdown();
        instanceMap.get(0).shutdown();
    }

    @Test
    public void fiveTimesTest2Instances1ClusterMemberAndFailover() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            test2Instances1ClusterMemberAndFailover();
        }
    }

    @Test(expected = RuntimeException.class)
    public void throwsRuntimeExceptionWhenNoMemberToConnect
            () throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        Map realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = newHazelcastClient(h1, h2, h3);
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
    public void addListenerWithTwoMemberClusterAndKillOne
            () throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = newHazelcastClient(h1, h2);
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
        assertTrue(entryAddLatch.await(10, SECONDS));
        assertTrue(entryUpdatedLatch.await(10, SECONDS));
        assertTrue(entryRemovedLatch.await(10, SECONDS));
    }

    @Test
    public void add2ListenerWithTwoMemberClusterRemoveOneListenerAndKillOneClusterInstance
            () throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = newHazelcastClient(h1, h2);
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
        Thread.sleep(1000);
        assertEquals(1, entryAddLatch.getCount());
        assertEquals(1, entryUpdatedLatch.getCount());
        assertEquals(1, entryRemovedLatch.getCount());
//        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void addMessageListenerWhithClusterFailOver
            () throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = newHazelcastClient(h1, h2);
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
        System.out.println("FINISHED");
        assertTrue(latch.await(5, SECONDS));
    }

    @Test
    public void shutdown
            () throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
            client = newHazelcastClient(h1);
            Map<String, String> clientMap = client.getMap("map1");
            Map<String, String> hMap = h1.getMap("map1");
            clientMap.put("A", String.valueOf(i));
            assertEquals(String.valueOf(i), hMap.get("A"));
        }
    }

    @Test
    public void testGetInstancesCreatedFromClient() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        client = newHazelcastClient(h);
        List list = client.getList("testGetInstancesCreatedFromClient");
        Map map = client.getMap("testGetInstancesCreatedFromClient");
        MultiMap mmap = client.getMultiMap("testGetInstancesCreatedFromClient");
        Queue q = client.getQueue("testGetInstancesCreatedFromClient");
        Set set = client.getSet("testGetInstancesCreatedFromClient");
        ITopic topic = client.getTopic("testGetInstancesCreatedFromClient");
        Lock lock = client.getLock("testGetInstancesCreatedFromClient");
        Collection<Instance> caches = client.getInstances();
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
        assertEquals(2, client.getInstances().size());
        mmap.put("key", "value1");
        q.offer("Element");
        assertEquals(4, client.getInstances().size());
        set.add("element");
        topic.publish("Message");
        caches = client.getInstances();
        for (Iterator<Instance> instanceIterator = caches.iterator(); instanceIterator.hasNext();) {
            Instance instance = instanceIterator.next();
            assertTrue(instance.getId().toString().endsWith("testGetInstancesCreatedFromClient"));
            assertTrue(listOfInstances.contains(instance));
            instance.destroy();
        }
    }

    //ok to fail due to lock
    @Test
    @Ignore
    public void testGetInstancesCreatedFromCluster
            () {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
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
        client = newHazelcastClient(h);
        Collection<Instance> caches = client.getInstances();
        assertEquals(listOfInstances.size(), caches.size());
        for (Iterator<Instance> instanceIterator = caches.iterator(); instanceIterator.hasNext();) {
            Instance instance = instanceIterator.next();
            assertTrue(instance.getId().toString().endsWith("testGetInstancesCreatedFromCluster"));
            assertTrue(listOfInstances.contains(instance));
        }
        h.shutdown();
    }

    @Test
    public void testAuthenticate
            () {
        String grName = "dev";
        String grPass = "pass";
        Config conf = new Config();
        conf.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        client = HazelcastClient.newHazelcastClient(grName, grPass, true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
        client.shutdown();
        h.shutdown();
    }

    @Test
    public void testAuthenticateWithEmptyPass
            () {
        String grName = "dev";
        String grPass = "";
        Config conf = new Config();
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        client = HazelcastClient.newHazelcastClient(grName, "", true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
        client.shutdown();
        h.shutdown();
    }

    @Test(expected = RuntimeException.class)
    public void testAuthenticateWrongPass
            () {
        String grName = "dev";
        String grPass = "pass";
        Config conf = new Config();
        GroupConfig gc = new GroupConfig();
        gc.setName(grName);
        gc.setPassword(grPass);
        conf.setGroupConfig(gc);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(conf);
        client = HazelcastClient.newHazelcastClient(grName, "wrong-pass", true, h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort());
        Map map = client.getMap("aasd");
        client.shutdown();
        h.shutdown();
    }

    @Test
    public void addMemberShipListener
            () throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h);
        final CountDownLatch added = new CountDownLatch(1);
        final CountDownLatch removed = new CountDownLatch(1);
        final Map<String, Member> map = new HashMap<String, Member>();
        client.getCluster().addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                map.put("Added", membershipEvent.getMember());
                added.countDown();
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                map.put("Removed", membershipEvent.getMember());
                removed.countDown();
            }
        });
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Member member = h2.getCluster().getLocalMember();
        h2.shutdown();
        assertTrue(added.await(10, SECONDS));
        assertEquals(member.getInetSocketAddress(), map.get("Added").getInetSocketAddress());
        assertTrue(removed.await(10, SECONDS));
        assertEquals(member.getInetSocketAddress(), map.get("Removed").getInetSocketAddress());
    }

    @Test
    public void retrieveDataSerializableClass
            () throws InterruptedException {
        System.out.println("Start");
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h);
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
        assertTrue(cdl.await(2, SECONDS));
    }

    @Test
    public void clientWithAutoMemberListUpdate
            () throws InterruptedException {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        client = getAutoUpdatingClient(h1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Thread.sleep(1000);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {

            public void run() {
                h1.shutdown();
                latch.countDown();
            }
        }).start();
        Map<Integer, Integer> map = client.getMap("map");
        map.put(1, 1);
        assertEquals(Integer.valueOf(1), map.get(1));
        latch.await();
        map.put(2, 2);
        assertEquals(Integer.valueOf(2), map.get(2));
    }

    @Test
    public void ensureClientWillUpdateMembersList
            () throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        client = getAutoUpdatingClient(h1);
        Thread.sleep(1000);
        h1.shutdown();
        Map<Integer, Integer> map = client.getMap("map");
        map.put(1, 1);
        assertEquals(Integer.valueOf(1), map.get(1));
    }

    private HazelcastClient getAutoUpdatingClient(HazelcastInstance h1) {
        Config conf = h1.getConfig();
        HazelcastClient client = HazelcastClient.newHazelcastClient(conf.getGroupConfig().getName(), conf.getGroupConfig().getPassword(),
                h1.getCluster().getLocalMember().getInetSocketAddress().toString().substring(1));
        return client;
    }

    @Test(timeout = 25000, expected = MemberLeftException.class)
    public void shouldThrowMemberLeftExcWhenNotConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h2);
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
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h2);
        Set<Member> members = client.getCluster().getMembers();
        MultiTask<Long> task =
                new MultiTask<Long>(new SleepCallable(10000), members);
        client.getExecutorService().submit(task);
        Thread.sleep(2000);
        h2.shutdown();
        task.get();
    }

    @Test
    public void shouldThrowNoMemeberAvailableExceptionWhenThereIsNoMemberToConnect() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h);
        final Map<Integer, Integer> map = client.getMap("default");
        map.put(1, 1);
        h.shutdown();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {

            public void run() {
                try {
                    client.getCluster().getMembers();
                } catch (NoMemberAvailableException e) {
                    latch.countDown();
                }
            }
        }).start();
        latch.await(10, SECONDS);
    }

    @Test(timeout = 25000, expected = MemberLeftException.class)
    public void shouldThrowMemberLeftExceptionWhenConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h1, h2);
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
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h);
        Future future1 = client.getExecutorService().submit(new SleepCallable(10000));
        Future future2 = client.getExecutorService().submit(new SleepCallable(10000));
        Thread.sleep(200);
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
        latch.await(10, SECONDS);
        client.shutdown();
        config.setGroupConfig(new GroupConfig());
    }

    @Test(expected = NoMemberAvailableException.class)
    public void getClusterInFailOver() throws InterruptedException {
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignore) {
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
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Config config2 = new XmlConfigBuilder().build();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        GroupConfig gConfig1 = new GroupConfig("g1", "pg1");
        GroupConfig gConfig2 = new GroupConfig("g2", "pg2");
        config1.setGroupConfig(gConfig1);
        config2.setGroupConfig(gConfig2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        HazelcastInstance client1 = newHazelcastClient(h1);
        HazelcastInstance client2 = newHazelcastClient(h2);
        Transaction t1 = client1.getTransaction();
        Transaction t2 = client2.getTransaction();
        t1.begin();
        client1.getMap("map").put(1, 4);
        t1.commit();
        client1.shutdown();
        client2.shutdown();
        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void multiTaskWithTwoMember() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = newHazelcastClient(h2);
        ExecutorService esService = client.getExecutorService();
        Set<Member> members = client.getCluster().getMembers();
        MultiTask<DistributedMapStatsCallable.MemberMapStat> task =
                new MultiTask<DistributedMapStatsCallable.MemberMapStat>(new DistributedMapStatsCallable("default"), members);
        esService.submit(task);
        Collection<DistributedMapStatsCallable.MemberMapStat> mapStats = null;
        mapStats = task.get();
        for (DistributedMapStatsCallable.MemberMapStat memberMapStat : mapStats) {
            assertNotNull(memberMapStat);
        }
        assertEquals(members.size(), mapStats.size());
    }

    @Test(expected = ExecutionException.class)
    public void shouldThrowExceptionWhenCallableThrowsException() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = null;
        try {
            h1 = Hazelcast.newHazelcastInstance(config);
            client = newHazelcastClient(h1);
            Set<Member> members = client.getCluster().getMembers();
            MultiTask<String> task =
                    new MultiTask<String>(new ExceptionThrowingCallable(), members);
            client.getExecutorService().submit(task);
            Collection<String> result = null;
            result = task.get();
            assertEquals(members.size(), result.size());
        } finally {
            h1.shutdown();
        }
    }

    @Test
    public void testClientHangOnShutdown() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        final HazelcastClient client = newHazelcastClient(h);
        final IMap map = client.getMap("default");
        map.lock("1");
        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                l1.countDown();
                try {
                    map.lock("1");
                    fail("Should not lock!!");
                } catch (Exception e) {
                    l2.countDown();
                }
            }
        }).start();
        l1.await(10, SECONDS);
        Thread.sleep(5000);
        h.shutdown();
        assertTrue(l2.await(10, SECONDS));
    }

    @Test
    public void throwsRuntimeExceptionWhenNoMemberToConnectTrwyWithLocks() {
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
            HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
            Map realMap = h1.getMap("default");
            Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1);
            client = newHazelcastClient(h1, h3);
            Map map = client.getMap("default");
            int counter = 0;
            realMap.get("currentIteratedKey");
            while (counter < 2) {
                map.put("currentIteratedKey", counter);
                assertEquals(counter, map.get("currentIteratedKey"));
                assertEquals(counter, realMap.get("currentIteratedKey"));
                memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
                counter++;
            }
        } catch (Exception e) {
            System.out.println("Here is the exception: " + e);
        }
    }

    @Test
    public void twoQueueInstancesEqual() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        client = newHazelcastClient(h1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IQueue<String> queue = client.getQueue("equals");
        assertEquals(queue, h2.getQueue("equals"));
    }

    public static class ExceptionThrowingCallable implements Callable<String>, Serializable {

        public String call() throws Exception {
            throw new RuntimeException("here is an exception");
        }
    }

    @Test
    public void shutdownClientOnNoMemberAvailable() throws InterruptedException {
        Thread[] initialThreads = getAllThreads();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = newHazelcastClient(h);
        client.getCluster().getMembers();
        h.shutdown();
        Thread.sleep(1000);
        Thread[] threads = getAllThreads();
        List<Thread> listOfThreads = new ArrayList<Thread>(Arrays.asList(threads));
        for (Thread thread : initialThreads) {
            listOfThreads.remove(thread);
        }
        boolean fail = false;
        for (Thread t : listOfThreads) {
            if (t != null) {
                if (t.getName().startsWith("hz.")) {
                    System.out.println("Thread is active " + t.getName());
                    fail = true;
                }
            }
        }
        assertFalse("Some threads are still alive", fail);
    }

    @Test
    public void lockIMapAndGetInstancesFromClient() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        final IMap iMap = h.getMap("map");
        iMap.put("key", "value");
        ILock lock = h.getLock(iMap);
        lock.lock();
        final HazelcastClient client = newHazelcastClient(h);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                client.getInstances();
                latch.countDown();
            }
        }).start();
        assertTrue("Could not get instances from client", latch.await(1, SECONDS));
    }

    @Test(timeout = 15000)
    public void mapLockFromClientAndThenCrashClientShouldReleaseLock() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = newHazelcastClient(h);
        client.getMap("def").lock("1");
        client.shutdown();
        h.getMap("def").lock("1");
        h.shutdown();
    }

    @Test(timeout = 15000)
    public void lockFromClientAndThenCrashClientShouldReleaseLock() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = newHazelcastClient(h);
        Lock lock = client.getLock("1");
        lock.lock();
        client.shutdown();
        h.getLock("1").lock();
        h.shutdown();
    }

    @Test
    public void clientEndpointShouldbeRemovedAfterClientShutDown() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        assertEquals(0, getNumberOfClientsConnected(h));
        for (int i = 0; i < 10; i++) {
            if (i % 10 == 0) {
                System.out.println(i);
            }
            HazelcastClient client = newHazelcastClient(h);
            System.out.println("Client " + client);
            assertEquals(1, getNumberOfClientsConnected(h));
            client.shutdown();
        }
        Thread.sleep(1000);
        assertEquals(0, getNumberOfClientsConnected(h));
        h.shutdown();
    }

    @Test
    public void create10clientsThenShutdownNumberOfConnectedClientsShouldBeZero() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        assertEquals(0, getNumberOfClientsConnected(h));
        List<HazelcastClient> listOfHazelcastClient = new ArrayList<HazelcastClient>();
        for (int i = 0; i < 10; i++) {
            HazelcastClient client = newHazelcastClient(h);
            listOfHazelcastClient.add(client);
        }
        assertEquals(10, getNumberOfClientsConnected(h));
        for (HazelcastClient client : listOfHazelcastClient) {
            client.shutdown();
        }
        Thread.sleep(1000);
        assertEquals(0, getNumberOfClientsConnected(h));
        h.shutdown();
    }

    @Test
    public void afterClientTerminationListenersAttachedByItShouldBeRemovedFromMember() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = newHazelcastClient(h);
        String mapName = "afterClientTerminationListenersAttachedByItShouldBeRemovedFromMember";
        IMap clientMap = client.getMap(mapName);
        clientMap.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent entryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void entryRemoved(EntryEvent entryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void entryUpdated(EntryEvent entryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void entryEvicted(EntryEvent entryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        }, true);
        IMap map = h.getMap(mapName);
        map.put("1", "1");
        Thread.sleep(100);
        assertEquals(1, map.getLocalMapStats().getOperationStats().getNumberOfEvents());
        client.shutdown();
        map.put(2, 2);
        Thread.sleep(1000);
        assertEquals(1, map.getLocalMapStats().getOperationStats().getNumberOfEvents());
    }

    private int getNumberOfClientsConnected(HazelcastInstance h) {
        FactoryImpl.HazelcastInstanceProxy proxy = (FactoryImpl.HazelcastInstanceProxy) h;
        FactoryImpl factory = (FactoryImpl) proxy.getHazelcastInstance();
        int size = factory.node.clientService.numberOfConnectedClients();
        return size;
    }
}
