/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.client.longrunning;

import com.hazelcast.client.*;
import com.hazelcast.client.ClientProperties.ClientPropertyName;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.SleepCallable;
import com.hazelcast.monitor.DistributedMapStatsCallable;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.client.HazelcastClientMapTest.getAllThreads;
import static com.hazelcast.client.TestUtility.getAutoUpdatingClient;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

public class DynamicClusterTest {
    HazelcastClient client;
    Config config = new Config();

    @Before
    public void before() throws Exception {
        System.setProperty("junit.default.timeout", "300000");
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList(Inet4Address.getLocalHost().getHostName()));
    }

    @After
    @Before
    public void after() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    //@After
    public void afterAwait() throws Exception {
        System.err.println("-------------------");
        System.in.read();
    }

    @Test
    public void continuePutAndGetIfOneOfConnectedClusterMemberFails() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h1, h2);
        Map realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        Map map = client.getMap("default");
        int counter = 0;
        while (counter < 2) {
            try {
                map.put("currentIteratedKey", counter);
            } catch (Throwable e) {
                fail(e.getMessage());
            }
            assertEquals(counter, realMap.get("currentIteratedKey"));
            assertEquals(counter, map.get("currentIteratedKey"));
            final int port = client.getConnectionManager().getConnection().getAddress().getPort();
            memberMap.get(port).shutdown();
            counter++;
        }
        h3.shutdown();
    }

    @Test
    public void fiveTimesContinuePutAndGetIfOneOfConnectedClusterMemberFails() throws Exception {
        for (int i = 0; i < 5; i++) {
            continuePutAndGetIfOneOfConnectedClusterMemberFails();
        }
    }

    @Test
    public void testClientFailover() throws InterruptedException {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = TestUtility.newHazelcastClient(h2, h1);
        Map map = client.getMap("myMap");
        int i = 0;
        while (i < 100) {
            map.put("test", ++i);
            if (i == 20) {
                h2.shutdown();
            }
        }
        assertEquals(i, map.get("test"));
    }

    @Test
    public void testClientFailoverWithOneMember() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastClient client = TestUtility.newHazelcastClient(h1);
        Map map = client.getMap("myMap");
        map.put("test", 1);
        h1.getLifecycleService().shutdown();
        Thread.sleep(1000);
        h1 = Hazelcast.newHazelcastInstance(config);

        assertEquals(null, map.get("test"));
        map.put("test", 2);
        assertEquals(2, map.get("test"));

        h1.getLifecycleService().shutdown();
    }

    @Test(expected = RuntimeException.class)
    public void throwsRuntimeExceptionWhenNoMemberToConnect
            () throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        Map realMap = h3.getMap("default");
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = TestUtility.newHazelcastClient(h1, h2, h3);
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
        client = TestUtility.newHazelcastClient(h1, h2);
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
        map.size();
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, SECONDS));
        assertTrue(entryUpdatedLatch.await(10, SECONDS));
        assertTrue(entryRemovedLatch.await(10, SECONDS));
    }

    @Test
    public void addListenerWithTwoMemberClusterAndKillOnePutFromNodes
            () throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        client = TestUtility.newHazelcastClient(h1, h2);
        IMap<String, String> clientMap = client.getMap("default");
        IMap<String, String> map1 = h1.getMap("default");
        IMap<String, String> map2 = h2.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(2);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(2);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(2);
        CountDownLatchEntryListener<String, String> listener = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        clientMap.addEntryListener(listener, true);
        map1.put("hello", "world");
        map1.put("hello", "new world");
        map1.remove("hello");
        memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
        clientMap.size();
        clientMap.put("hello", "world");
        clientMap.put("hello", "new world");
        clientMap.remove("hello");
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
        client = TestUtility.newHazelcastClient(h1, h2);
        IMap<String, String> map = client.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(3);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(3);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(3);
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
        assertTrue(entryAddLatch.await(5000, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(5000, TimeUnit.MILLISECONDS));
        assertTrue(entryRemovedLatch.await(5000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void addMessageListenerWithClusterFailOver() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h1, h2);
        final ITopic<String> topic = client.getTopic("ABC");
        final CountDownLatch latch = new CountDownLatch(3);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.publish(message);
        Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1, h2);
        HazelcastInstance h = memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort());
        h.shutdown();
        Thread.sleep(1000);
        for (int i = 0; i < 2; i++) {
            new Thread(new Runnable() {
                public void run() {
                    topic.publish(message);
                }
            }).start();
        }
        assertTrue(latch.await(5, SECONDS));
    }

    @Test
    public void testShutdown() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        for (int i = 0; i < 3; i++) {
            client = TestUtility.newHazelcastClient(h1);
            Map<String, String> clientMap = client.getMap("map1");
            Map<String, String> hMap = h1.getMap("map1");
            clientMap.put("A", String.valueOf(i));
            assertEquals(String.valueOf(i), hMap.get("A"));
            client.shutdown();
        }
    }

    @Test
    public void testGetInstancesCreatedFromClient() {
        String instanceName = "testGetInstancesCreatedFromClient";
        HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        client = TestUtility.newHazelcastClient(h);
        List list = client.getList(instanceName);
        Map map = client.getMap(instanceName);
        MultiMap mmap = client.getMultiMap(instanceName);
        Queue q = client.getQueue(instanceName);
        Set set = client.getSet(instanceName);
        ITopic topic = client.getTopic(instanceName);
        Lock lock = client.getLock(instanceName);
        List listOfInstances = new ArrayList();
        listOfInstances.add(list);
        listOfInstances.add(map);
        listOfInstances.add(mmap);
        listOfInstances.add(q);
        listOfInstances.add(set);
        listOfInstances.add(topic);
        listOfInstances.add(lock);
        assertEquals(0, client.getInstances().size());
        list.add("List");
        map.put("key", "value");
        assertEquals(2, client.getInstances().size());
        mmap.put("key", "value1");
        q.offer("Element");
        assertEquals(4, client.getInstances().size());
        set.add("element");
        topic.publish("Message");
        assertEquals(6, client.getInstances().size());
        for (Instance instance : client.getInstances()) {
            assertTrue(instance.getId().toString().endsWith(instanceName));
            instance.destroy();
        }
    }

    @Test
    public void testGetInstancesCreatedFromCluster
            () {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        IList list = h.getList("List");
        IMap map = h.getMap("IMap");
        MultiMap mmap = h.getMultiMap("MultiMap");
        IQueue q = h.getQueue("IQueue");
        ISet set = h.getSet("ISet");
        ITopic topic = h.getTopic("ITopic");
        ILock lock = h.getLock("ILock");
        List<Object> listOfInstanceIds = new ArrayList<Object>();
        listOfInstanceIds.add(list.getId());
        listOfInstanceIds.add(map.getId());
        listOfInstanceIds.add(mmap.getId());
        listOfInstanceIds.add(q.getId());
        listOfInstanceIds.add(set.getId());
        listOfInstanceIds.add(topic.getId());
        listOfInstanceIds.add(lock.getId());
        client = TestUtility.newHazelcastClient(h);
        Collection<Instance> caches = client.getInstances();
        assertEquals(listOfInstanceIds.size(), caches.size());
        for (Instance instance : caches) {
            assertTrue(listOfInstanceIds.contains(instance.getId()));
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
        client = newHazelcastClient(grName, grPass, h);
        Map map = client.getMap("aasd");
        client.shutdown();
        h.shutdown();
    }

    private HazelcastClient newHazelcastClient(String grName, String grPass, HazelcastInstance h) {
        String address = h.getCluster().getLocalMember().getInetSocketAddress().getAddress().getCanonicalHostName() + ":" + h.getCluster().getLocalMember().getInetSocketAddress().getPort();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(grName, grPass)).addAddress(address);
        return HazelcastClient.newHazelcastClient(clientConfig);
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
        client = newHazelcastClient(grName, "", h);
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
        client = newHazelcastClient(grName, "wrong-pass", h);
        Map map = client.getMap("aasd");
        client.shutdown();
        h.shutdown();
    }

    @Test
    public void addMemberShipListener
            () throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h);
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
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h);
        IMap<Integer, DataSerializableUser> clientMap = client.getMap("retreiveDataSerializableClass");
        final DataSerializableUser user = new DataSerializableUser();
        user.setName("name");
        user.setFamilyName("fName");
        user.setAge(30);
        user.setAddress(new DataSerializableUser.Address());
        final CountDownLatch cdl = new CountDownLatch(2);
        EntryListener<Integer, DataSerializableUser> listener = new EntryAdapter<Integer, DataSerializableUser>() {
            public void entryAdded(EntryEvent<Integer, DataSerializableUser> entryEvent) {
                DataSerializableUser u = entryEvent.getValue();
                assertEquals(user.getName(), u.getName());
                assertEquals(user.getFamilyName(), u.getFamilyName());
                assertEquals(user.getAge(), u.getAge());
                assertEquals(user.getAddress().getAddress(), u.getAddress().getAddress());
                cdl.countDown();
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
        Map<Integer, Integer> map = client.getMap("map");
        map.put(1, 1);
        assertEquals(Integer.valueOf(1), map.get(1));
        h1.shutdown();
        Thread.sleep(1000);
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

    @Test(timeout = 25000, expected = MemberLeftException.class)
    public void shouldThrowMemberLeftExcWhenNotConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h2);
        Set<Member> members = client.getCluster().getMembers();
        MultiTask<Long> task =
                new MultiTask<Long>(new SleepCallable(10000), members);
        client.getExecutorService().submit(task);
        Thread.sleep(2000);
        h1.shutdown();
        task.get();
    }

    @Test(timeout = 60000, expected = ExecutionException.class)
    public void shouldThrowExExcptnWhenTheOnlyConnectedMemberDiesWhileExecuting() throws ExecutionException, InterruptedException {
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h2);
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
        client = TestUtility.newHazelcastClient(h);
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
        client = TestUtility.newHazelcastClient(h1, h2);
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
        client = TestUtility.newHazelcastClient(h);
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
        client = TestUtility.newHazelcastClient(h);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignore) {
                }
                h.shutdown();
            }
        }).start();
        for (int i = 0; i < 10; i++) {
            client.getCluster().getMembers();
            Thread.sleep(2);
        }
        // should not reach here
        fail();
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
        HazelcastInstance client1 = TestUtility.newHazelcastClient(h1);
        HazelcastInstance client2 = TestUtility.newHazelcastClient(h2);
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
    public void rollbackTransactionWhenClientDies() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance client = TestUtility.newHazelcastClient(h);
        Transaction transaction = client.getTransaction();
        transaction.begin();
        Map<String, String> map = client.getMap("rollbackTransactionWhenClientDies");
        map.put("1", "A");
        client.getLifecycleService().shutdown();
        assertTrue(h.getMap("rollbackTransactionWhenClientDies").isEmpty());
        h.getMap("rollbackTransactionWhenClientDies").put("1", "B");
        assertEquals("B", h.getMap("rollbackTransactionWhenClientDies").get("1"));
    }

    @Test
    public void multiTaskWithTwoMember() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        client = TestUtility.newHazelcastClient(h2);
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
            client = TestUtility.newHazelcastClient(h1);
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
        final HazelcastClient client = TestUtility.newHazelcastClient(h);
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
        assertTrue(l2.await(100, SECONDS));
    }

    @Test
    public void throwsRuntimeExceptionWhenNoMemberToConnectTrwyWithLocks() {
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
            HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
            Map realMap = h1.getMap("default");
            Map<Integer, HazelcastInstance> memberMap = getMapOfClusterMembers(h1);
            client = TestUtility.newHazelcastClient(h1, h3);
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
        client = TestUtility.newHazelcastClient(h1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IQueue<String> queue = client.getQueue("equals");
        assertEquals(queue, h2.getQueue("equals"));
    }

    public static class ExceptionThrowingCallable implements Callable<String>, Serializable {

        public String call() throws Exception {
            throw new RuntimeException("here is an exception");
        }
    }

    @Test(timeout = 30000)
    public void testShutdownThreads() throws InterruptedException {
        Thread[] initialThreads = getAllThreads();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        final ClientProperties clientProperties =
                ClientProperties.createBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient client = TestUtility.newHazelcastClient(clientProperties, h);
        client.getCluster().getMembers();
        h.shutdown();
        try {
            client.getMap("default").put("1", "1");
            fail();
        } catch (NoMemberAvailableException e) {
        }
        client.shutdown();
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
        final HazelcastClient client = TestUtility.newHazelcastClient(h);
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
        HazelcastClient client = TestUtility.newHazelcastClient(h);
        client.getMap("def").lock("1");
        client.shutdown();
        h.getMap("def").lock("1");
        h.shutdown();
    }

    @Test(timeout = 15000)
    public void lockFromClientAndThenCrashClientShouldReleaseLock() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = TestUtility.newHazelcastClient(h);
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
            HazelcastClient client = TestUtility.newHazelcastClient(h);
            assertEquals(1, getNumberOfClientsConnected(h));
            client.shutdown();
        }
        Thread.sleep(5000);
        assertEquals(0, getNumberOfClientsConnected(h));
        h.shutdown();
    }

    @Test
    public void create10clientsThenShutdownNumberOfConnectedClientsShouldBeZero() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        assertEquals(0, getNumberOfClientsConnected(h));
        List<HazelcastClient> listOfHazelcastClient = new ArrayList<HazelcastClient>();
        for (int i = 0; i < 10; i++) {
            HazelcastClient client = TestUtility.newHazelcastClient(h);
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
        HazelcastClient client = TestUtility.newHazelcastClient(h);
        String mapName = "afterClientTerminationListenersAttachedByItShouldBeRemovedFromMember";
        IMap clientMap = client.getMap(mapName);
        clientMap.addEntryListener(new EntryAdapter(), true);
        IMap map = h.getMap(mapName);
        map.put("1", "1");
        Thread.sleep(100);
        assertEquals(1, map.getLocalMapStats().getOperationStats().getNumberOfEvents());
        client.shutdown();
        map.put(2, 2);
        Thread.sleep(5000);
        assertEquals(1, map.getLocalMapStats().getOperationStats().getNumberOfEvents());
    }

    @Test
    public void oneNode2Clients() throws InterruptedException {
        final AtomicBoolean finished = new AtomicBoolean(false);
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        int threadCount = 2;
        int operationCount = 100;
        final BlockingQueue queue = new LinkedBlockingQueue();
        for (int i = 0; i < operationCount; i++) {
            queue.add(i);
        }
        final CountDownLatch latch = new CountDownLatch(operationCount);
        final CountDownLatch startUpLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            new Thread(new Runnable() {
                public void run() {
                    int count = 0;
                    try {
                        System.out.println(Thread.currentThread() + " Running....");
//                        HazelcastClient client = newHazelcastClient(h);
                        HazelcastClient client;
                        synchronized (finished) {
                            client = TestUtility.newHazelcastClient("dev", "dev-pass", "localhost");
                        }
                        System.out.println(Thread.currentThread() + " Client init");
                        startUpLatch.countDown();
                        startUpLatch.await();
                        while (!finished.get()) {
                            queue.take();
                            client.getMap("map").put(latch.getCount(), latch.getCount());
                            count++;
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                    System.out.println(Thread.currentThread() + "processed: " + count);
                }
            }).start();
        }
        assertTrue(latch.await(200, TimeUnit.SECONDS));
        finished.set(true);
    }

    @Test
    public void performanceWithLotsOfExecutingTasks() throws InterruptedException, ExecutionException {
        Config config = new Config();
        config.addExecutorConfig(new ExecutorConfig("esname", 128, 512, 60));
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        for (int i = 0; i < 1000; i++) {
            Map<Integer, byte[]> map = h1.getMap("myMap");
            map.put(i, new byte[100000]);
        }
        HazelcastClient client = TestUtility.newHazelcastClient(h2);
        ExecutorService executor = client.getExecutorService("esname");
        Map<Integer, FutureTask> taskMap = new ConcurrentHashMap<Integer, FutureTask>();
        long start = Clock.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 1000; i++) {
            FutureTask<Integer> task = new DistributedTask<Integer>(
                    new MyTask(i), i);
            executor.execute(task);
            taskMap.put(i, task);
        }
        int counter = 0;
        for (Integer task : taskMap.keySet()) {
            int x = (Integer) (taskMap.get(task).get());
            assertEquals(task.intValue(), x);
            counter++;
        }
    }

    private int getNumberOfClientsConnected(HazelcastInstance h) {
        FactoryImpl.HazelcastInstanceProxy proxy = (FactoryImpl.HazelcastInstanceProxy) h;
        FactoryImpl factory = (FactoryImpl) proxy.getHazelcastInstance();
        int size = factory.node.clientHandlerService.numberOfConnectedClients();
        return size;
    }

    public static class MyTask extends HazelcastInstanceAwareObject implements Callable, Serializable {
        final int x;
        byte[] b = new byte[100];
        String str = "asdadsddad";

        public MyTask(int x) {
            this.x = x;
        }

        public Object call() throws Exception {
            Map<Integer, byte[]> map = hazelcastInstance.getMap("myMap");
            byte[] value = map.get(x);
            map.put(x, value);
            Thread.sleep(10);
            return x;
        }
    }

    @Test
    public void testClientCrashOnQTake() throws InterruptedException {
        Config config = new Config();
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        final HazelcastClient client = TestUtility.newHazelcastClient(h);
        final String qName = "testClientCrashOnQTake";
        final AtomicBoolean gotExpectedException = new AtomicBoolean(false);
        new Thread(new Runnable() {
            public void run() {
                try {
                    client.getQueue(qName).take();
                } catch (NoMemberAvailableException e) {
                    gotExpectedException.set(true);
                    return;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fail();
            }
        }).start();
        Thread.sleep(1000);
        client.shutdown();
        //Expect the client take operation to be thrown a NoMemberAvailableException and cancelled.
        assertTrue(gotExpectedException.get());
        assertEquals(0, h.getQueue(qName).size());
        h.getQueue(qName).offer("message");
        assertEquals(1, h.getQueue(qName).size());
        Thread.sleep(100);
        assertEquals(1, h.getQueue(qName).size());
        h.getQueue(qName).poll();
        assertEquals(0, h.getQueue(qName).size());
    }

    @Test
    public void splitBrain() throws Exception {
        boolean multicast = true;
        Config c1 = new Config();
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multicast);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multicast);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().clear();
        c1.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().setEnabled(true);
        Config c2 = new Config();
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multicast);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multicast);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().clear();
        c2.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().setEnabled(true);
        c1.getGroupConfig().setName("differentGroup");
        c2.getGroupConfig().setName("sameGroup");
        c1.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        c1.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        c2.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        c2.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        HazelcastClient client2 = TestUtility.newHazelcastClient(c2.getGroupConfig().getName(), c2.getGroupConfig().getPassword(), "127.0.0.1:5702");
        client2.getTopic("def").addMessageListener(new MessageListener<Object>() {
            public void onMessage(Message message) {
            }
        });
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);
        for (int i = 0; i < 500; i++) {
            h2.getMap("default").put(i, "value" + i);
            h2.getMultiMap("default").put(i, "value" + i);
            h2.getMultiMap("default").put(i, "value0" + i);
        }
        assertEquals(500, h2.getMap("default").size());
        assertEquals(1000, h2.getMultiMap("default").size());
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        Thread.sleep(2000);
        c1.getGroupConfig().setName("sameGroup");
        Assert.assertTrue(l.waitFor(LifecycleState.MERGED, 40));
        Assert.assertEquals(1, l.getCount(LifecycleState.MERGING));
        Assert.assertEquals(1, l.getCount(LifecycleState.RESTARTING));
        Assert.assertEquals(1, l.getCount(LifecycleState.RESTARTED));
        Assert.assertEquals(1, l.getCount(LifecycleState.MERGED));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(500, h1.getMap("default").size());
        assertEquals(500, h2.getMap("default").size());
        assertEquals(1000, h2.getMultiMap("default").size());
        assertEquals(1000, h1.getMultiMap("default").size());
        Thread.sleep(10000);
    }


    @Test(timeout = 60000, expected = NoMemberAvailableException.class)
    public void shouldNotBlockForever(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = HazelcastClient.newHazelcastClient(new ClientConfig());
        Map map = client.getMap("shouldNotBlockForever");
        map.put("1", "a");
        h.getLifecycleService().shutdown();
        map.put("1", "b");
    }

    @Test
    public void testUpdateEventOrder() throws InterruptedException {
        Config config = new Config();
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        //HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counterOfFails = new AtomicInteger(0);
        final AtomicReference<Integer> last = new AtomicReference<Integer>(0);
        HazelcastClient client = TestUtility.newHazelcastClient(h);
        client.getMap("testUpdateEventOrder").addEntryListener(new EntryListener<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                this.entryUpdated(event);
            }

            public void entryRemoved(EntryEvent<Object, Object> objectObjectEntryEvent) {
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                if ((Integer) event.getValue() - last.get() != 1) {
                    counterOfFails.incrementAndGet();
                }
                last.set((Integer) event.getValue());
            }

            public void entryEvicted(EntryEvent<Object, Object> objectObjectEntryEvent) {
            }
        }, true);
        ExecutorService ex = Executors.newFixedThreadPool(2);
        int count = 10000;
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            ex.execute(new Runnable() {
                public void run() {
                    synchronized (latch) {
                        h.getMap("testUpdateEventOrder").put("key", counter1.incrementAndGet());
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
    }

    class LifecycleCountingListener implements LifecycleListener {
        Map<LifecycleEvent.LifecycleState, AtomicInteger> counter = new ConcurrentHashMap<LifecycleEvent.LifecycleState, AtomicInteger>();
        BlockingQueue<LifecycleEvent.LifecycleState> eventQueue = new LinkedBlockingQueue<LifecycleEvent.LifecycleState>();

        LifecycleCountingListener() {
            for (LifecycleEvent.LifecycleState state : LifecycleEvent.LifecycleState.values()) {
                counter.put(state, new AtomicInteger(0));
            }
        }

        public void stateChanged(LifecycleEvent event) {
            counter.get(event.getState()).incrementAndGet();
            eventQueue.offer(event.getState());
        }

        int getCount(LifecycleEvent.LifecycleState state) {
            return counter.get(state).get();
        }

        boolean waitFor(LifecycleEvent.LifecycleState state, int seconds) {
            long remainingMillis = TimeUnit.SECONDS.toMillis(seconds);
            while (remainingMillis >= 0) {
                LifecycleEvent.LifecycleState received = null;
                try {
                    long now = Clock.currentTimeMillis();
                    received = eventQueue.poll(remainingMillis, TimeUnit.MILLISECONDS);
                    remainingMillis -= (Clock.currentTimeMillis() - now);
                } catch (InterruptedException e) {
                    return false;
                }
                if (received != null && received == state) {
                    return true;
                }
            }
            return false;
        }
    }
}
