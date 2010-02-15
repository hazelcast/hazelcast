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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

/**
 * Run these tests with
 * -Xms512m -Xmx512m
 */
public class ClusterTest {

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIdle() throws Exception {
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setMaxIdleSeconds(3);
        mapConfig.setEvictionDelaySeconds(10);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        Map map = h1.getMap("default");
        map.put(1, 1);
        assertEquals(1, map.get(1));
        Thread.sleep(2000);
        assertEquals(1, map.get(1));
        Thread.sleep(2000);
        assertEquals(1, map.get(1));
        Thread.sleep(4000);
        assertNull(map.get(1));
        assertEquals(0, map.size());
    }

    @Test
    public void testFirstNodeNoWait() throws Exception {
        final Config config = new XmlConfigBuilder().build();
        final BlockingQueue<Integer> counts = new ArrayBlockingQueue<Integer>(2);
        for (int j = 0; j < 2; j++) {
            new Thread(new Runnable() {
                public void run() {
                    final HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                    for (int i = 0; i < 3000; i++) {
                        h.getMap("default").put(i, "value");
                    }
                    counts.offer(getLocalPartitions(h).size());
                }
            }).start();
        }
        int first = counts.take();
        int second = counts.take();
        assertTrue(first == 0 || first == 271);
        assertTrue(second == 0 || second == 271);
        assertEquals(271, Math.abs(second - first));
    }

    @Test
    public void testFirstNodeWait() throws Exception {
        final Config config = new XmlConfigBuilder().build();
        config.setProperty(GroupProperties.PROP_FIRST_MEMBER_WAIT_SECONDS, "7");
        final CountDownLatch latch = new CountDownLatch(2);
        for (int j = 0; j < 2; j++) {
            new Thread(new Runnable() {
                public void run() {
                    final HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                    for (int i = 0; i < 3000; i++) {
                        h.getMap("default").put(i, "value");
                    }
                    assertTrue(getLocalPartitions(h).size() > 134);
                    latch.countDown();
                }
            }).start();
        }
        latch.await(20, TimeUnit.SECONDS);
    }

    private Set<Partition> getLocalPartitions(HazelcastInstance h) {
        Set<Partition> partitions = h.getPartitionService().getPartitions();
        Set<Partition> localPartitions = new HashSet<Partition>();
        for (Partition partition : partitions) {
            if (h.getCluster().getLocalMember().equals(partition.getOwner())) {
                localPartitions.add(partition);
            }
        }
        return localPartitions;
    }

    @Test(timeout = 10000, expected = RuntimeException.class)
    public void testPutAfterShutdown() throws InterruptedException {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        Map map = h1.getMap("default");
        h1.shutdown();
        map.put("1", "value");
    }

    @Test(timeout = 20000)
    public void testSuperClientPartitionOwnership() {
        Config config = new XmlConfigBuilder().build();
        config.setSuperClient(true);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(config);
        Map map = hSuper.getMap("default");
        assertNull(map.put("1", "value"));
        hNormal.shutdown();
        Set<Partition> partitions = hSuper.getPartitionService().getPartitions();
        for (Partition partition : partitions) {
            assertNull(partition.getOwner());
        }
        hNormal = Hazelcast.newHazelcastInstance(null);
        partitions = hSuper.getPartitionService().getPartitions();
        for (Partition partition : partitions) {
            assertEquals(hNormal.getCluster().getLocalMember(), partition.getOwner());
        }
        assertNull(map.put("1", "value"));
        hSuper.shutdown();
        partitions = hNormal.getPartitionService().getPartitions();
        for (Partition partition : partitions) {
            assertEquals(hNormal.getCluster().getLocalMember(), partition.getOwner());
        }
        assertEquals("value", hNormal.getMap("default").get("1"));
    }

    @Test(timeout = 10000)
    public void testPutAfterRestart() {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        Map map = h1.getMap("default");
        h1.restart();
        map.put("1", "value");
    }

    @Test(timeout = 30000)
    public void testSuperClientPutAfterBeforeNormalMember() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latchSuperPut = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                Config config = new XmlConfigBuilder().build();
                config.setSuperClient(true);
                final HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(config);
                latch.countDown();
                Map map = hSuper.getMap("default");
                map.put("1", "value");
                latchSuperPut.countDown();
            }
        }).start();
        latch.await(10, TimeUnit.SECONDS);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(null);
        assertTrue(latchSuperPut.await(10, TimeUnit.SECONDS));
        assertEquals("value", hNormal.getMap("default").get("1"));
    }

    @Test(timeout = 60000)
    public void testRestart() throws Exception {
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        IMap map = h.getMap("default");
        map.put("1", "value");
        final CountDownLatch latch = new CountDownLatch(1);
        Thread interrupter = new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(1000);
                    h.restart();
                    latch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        interrupter.start();
        map.put("1", "value");
        latch.await();
    }

    @Test(timeout = 60000)
    public void testRestart2() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map = h2.getMap("default");
        map.put("1", "value1");
        assertEquals(2, h.getCluster().getMembers().size());
        h2.restart();
        Thread.sleep(400);
        assertEquals("value1", map.get("1"));
        map.put("1", "value2");
        assertEquals("value2", map.get("1"));
        assertEquals("value2", h.getMap("default").get("1"));
    }

    @Test(timeout = 60000)
    public void testBackupCount() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 10000; i++) {
            map1.put(i, i);
        }
        assertEquals(map1.getLocalMapStats().getBackupEntryCount(), map2.getLocalMapStats().getOwnedEntryCount());
        assertEquals(map2.getLocalMapStats().getBackupEntryCount(), map1.getLocalMapStats().getOwnedEntryCount());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        IMap map3 = h3.getMap("default");
        assertEquals(map2.getLocalMapStats().getBackupEntryCount(), map1.getLocalMapStats().getOwnedEntryCount());
        assertEquals(map1.getLocalMapStats().getBackupEntryCount(), map3.getLocalMapStats().getOwnedEntryCount());
        assertEquals(map3.getLocalMapStats().getBackupEntryCount(), map2.getLocalMapStats().getOwnedEntryCount());
    }

    @Test(timeout = 60000)
    public void testMapReplaceIfSame() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        map1.put("1", "value1");
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals("value1", map1.get("1"));
        assertEquals("value1", map2.get("1"));
        assertTrue(map2.replace("1", "value1", "value2"));
        assertTrue(map1.replace("1", "value2", "value3"));
    }

    @Test
    public void testLockInstance() {
        ILock lock = Hazelcast.getLock("testLock");
        lock.lock();
        Collection<Instance> instances = Hazelcast.getInstances();
        boolean found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.LOCK) {
                ILock lockInstance = (ILock) instance;
                if (lockInstance.getLockObject().equals("testLock")) {
                    found = true;
                }
            }
        }
        Assert.assertTrue(found);
        instances = Hazelcast.getInstances();
        found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.LOCK) {
                ILock lockInstance = (ILock) instance;
                if (lockInstance.getLockObject().equals("testLock2")) {
                    found = true;
                }
            }
        }
        assertFalse(found);
        Hazelcast.getLock("testLock2");
        instances = Hazelcast.getInstances();
        found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.LOCK) {
                ILock lockInstance = (ILock) instance;
                if (lockInstance.getLockObject().equals("testLock2")) {
                    found = true;
                }
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testPutIfAbsentWhenThereIsTTL() throws InterruptedException {
        String mapName = "testTTL";
        int ttl = 1;
        Config myConfig = configTTLForMap(mapName, ttl);
        Hazelcast.init(myConfig);
        IMap<String, String> myMap = Hazelcast.getMap(mapName);
        String key = "1";
        String value = "value1";
        myMap.put(key, value);
        assertEquals(value, myMap.get(key));
        assertTrue(myMap.containsKey(key));
        Thread.sleep((ttl + 1) * 1000);
        assertFalse(myMap.containsKey(key));
        assertNull(myMap.get(key));
        assertNull(myMap.putIfAbsent(key, "value2"));
    }

    @Test
    public void testPutIfAbsentWhenThereIsTTLAndRemovedBeforeTTL() throws InterruptedException {
        String mapName = "testTTL";
        int ttl = 1;
        Config myConfig = configTTLForMap(mapName, ttl);
        Hazelcast.init(myConfig);
        IMap<String, String> myMap = Hazelcast.getMap(mapName);
        String key = "1";
        String value = "value1";
        myMap.put(key, value);
        assertEquals(value, myMap.get(key));
        assertTrue(myMap.containsKey(key));
        assertEquals(value, myMap.remove(key));
        Thread.sleep((ttl + 1) * 1000);
        assertFalse(myMap.containsKey(key));
        assertNull(myMap.get(key));
        assertNull(myMap.putIfAbsent(key, "value2"));
    }

    private Config configTTLForMap(String mapName, int ttl) {
        Config myConfig = new Config();
        Map<String, MapConfig> myHazelcastMapConfigs = myConfig.getMapMapConfigs();
        MapConfig myMapConfig = myHazelcastMapConfigs.get(mapName);
        if (myMapConfig == null) {
            myMapConfig = new MapConfig();
            myMapConfig.setName(mapName);
            myMapConfig.setTimeToLiveSeconds(ttl);
            myHazelcastMapConfigs.put(mapName, myMapConfig);
        } else {
            myMapConfig.setTimeToLiveSeconds(ttl);
        }
        return myConfig;
    }

    @Test(timeout = 60000)
    public void testDifferentGroups() {
        Config c1 = new XmlConfigBuilder().build();
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c1.getNetworkConfig().getInterfaces().clear();
        c1.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().setEnabled(true);
        Config c2 = new XmlConfigBuilder().build();
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getInterfaces().clear();
        c2.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().setEnabled(true);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        Hazelcast.shutdownAll();
        c2.getGroupConfig().setName("differentGroup");
        h1 = Hazelcast.newHazelcastInstance(c1);
        h2 = Hazelcast.newHazelcastInstance(c2);
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 60000)
    public void shutdownSuperClient() {
        Config c1 = new XmlConfigBuilder().build();
        c1.setPortAutoIncrement(false);
        c1.setPort(5709);
        Config c2 = new XmlConfigBuilder().build();
        c2.setPortAutoIncrement(false);
        c2.setPort(5710);
        c2.setSuperClient(true);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(c2);
        hNormal.getMap("default").put("1", "first");
        assert hSuper.getMap("default").get("1").equals("first");
        hNormal.shutdown();
        hSuper.shutdown();
    }

    @Test(timeout = 60000)
    public void testSuperClientRestart() throws Exception {
        Config configNormal = new XmlConfigBuilder().build();
        Config configSuper = new XmlConfigBuilder().build();
        configSuper.setSuperClient(true);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(configNormal);
        HazelcastInstance s = Hazelcast.newHazelcastInstance(configSuper);
        assertEquals(2, h.getCluster().getMembers().size());
        assertEquals(2, s.getCluster().getMembers().size());
        assertFalse(h.getCluster().getLocalMember().isSuperClient());
        assertTrue(s.getCluster().getLocalMember().isSuperClient());
        IMap map = h.getMap("default");
        final IMap maps = s.getMap("default");
        assertNull(map.put("1", "value1"));
        assertEquals("value1", map.get("1"));
        assertEquals("value1", maps.get("1"));
        assertEquals(1, map.size());
        assertEquals(1, maps.size());
        h.shutdown();
        Thread.sleep(500);
        assertEquals(1, s.getCluster().getMembers().size());
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                maps.size();
                assertNull(maps.get("1"));
                maps.put("1", "value3");
                latch.countDown();
            }
        }).start();
        h = Hazelcast.newHazelcastInstance(configNormal);
        latch.await();
        assertEquals(2, h.getCluster().getMembers().size());
        assertEquals(2, s.getCluster().getMembers().size());
        assertFalse(h.getCluster().getLocalMember().isSuperClient());
        assertTrue(s.getCluster().getLocalMember().isSuperClient());
        map = h.getMap("default");
        assertEquals("value3", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        assertEquals(1, maps.size());
    }

    @Test(timeout = 60000)
    public void testTcpIp() throws Exception {
        Config c = new XmlConfigBuilder().build();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addAddress(new Address("127.0.0.1", 5701));
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
    }

    @Test(timeout = 60000)
    public void testMulticast() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        testTwoNodes(h1, h2);
    }

    private void testTwoNodes(HazelcastInstance h1, HazelcastInstance h2) throws Exception {
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals("value2", h2.getMap("default").get("1"));
        assertEquals("value2", h1.getMap("default").get("1"));
        assertEquals(1, h1.getMap("default").size());
        assertEquals(1, h2.getMap("default").size());
        assertFalse(map1.containsKey("2"));
        assertFalse(map2.containsKey("2"));
        assertFalse(map1.containsValue("value1"));
        assertFalse(map2.containsValue("value1"));
        assertTrue(map1.containsKey("1"));
        assertTrue(map2.containsKey("1"));
        assertTrue(map1.containsValue("value2"));
        assertTrue(map2.containsValue("value2"));
        map1.lock("1");
        assertFalse(map2.tryLock("1"));
        map1.unlock("1");
        assertTrue(map2.tryLock("1"));
        map2.unlock("1");
        assertEquals("value2", map1.putIfAbsent("1", "value1"));
        assertEquals("value2", map2.putIfAbsent("1", "value1"));
        assertEquals("value2", map1.get("1"));
        assertEquals("value2", map2.get("1"));
        assertNull(map1.putIfAbsent("3", "value3"));
        assertEquals("value3", map1.get("3"));
        assertEquals("value3", map2.get("3"));
        assertEquals("value3", map2.remove("3"));
        assertNull(map1.get("3"));
        assertNull(map2.get("3"));
        assertNull(map2.putIfAbsent("3", "value3"));
        assertEquals("value3", map1.get("3"));
        assertEquals("value3", map2.get("3"));
        assertEquals("value3", map1.remove("3"));
        assertNull(map1.get("3"));
        assertNull(map2.get("3"));
        assertEquals(1, map1.keySet().size());
        assertEquals(1, map1.values().size());
        assertEquals(1, map1.entrySet().size());
        assertEquals(1, map2.keySet().size());
        assertEquals(1, map2.values().size());
        assertEquals(1, map2.entrySet().size());
        Set<Map.Entry> entries = map1.entrySet();
        for (Map.Entry entry : entries) {
            assertEquals("1", entry.getKey());
            assertEquals("value2", entry.getValue());
        }
        entries = map2.entrySet();
        for (Map.Entry entry : entries) {
            assertEquals("1", entry.getKey());
            assertEquals("value2", entry.getValue());
        }
        allMapListenerTest(map2, "5", map1);
    }

    @Test(timeout = 60000)
    public void testListeners2() throws Exception {
        final CountDownLatch latchAdded = new CountDownLatch(1);
        final CountDownLatch latchUpdated = new CountDownLatch(1);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchEvicted = new CountDownLatch(0);
        EntryListener listener = new EntryListener() {
            public void entryAdded(EntryEvent entryEvent) {
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent entryEvent) {
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent entryEvent) {
                latchUpdated.countDown();
            }

            public void entryEvicted(EntryEvent entryEvent) {
                latchEvicted.countDown();
            }
        };
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap mapSource = h2.getMap("default");
        IMap map = h1.getMap("default");
        Object key = "2133aa";
        map.addEntryListener(listener, key, true);
        assertNull(mapSource.put(key, "value5"));
        assertEquals("value5", mapSource.put(key, "value55"));
        assertFalse(mapSource.evict(key));
        assertEquals("value55", mapSource.put(key, "value5"));
        assertEquals("value5", mapSource.remove(key));
        map.removeEntryListener(listener, key);
        assertTrue(mapSource.evict(key));
        assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
        assertTrue(latchUpdated.await(5, TimeUnit.SECONDS));
        assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
        assertTrue(latchEvicted.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testListeners() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        allMapListenerTest(h2.getMap("default"), "5", h1.getMap("default"));
    }

    private void allMapListenerTest(IMap map, Object keyToUpdate, IMap mapSource) throws Exception {
        final CountDownLatch latchAdded = new CountDownLatch(2);
        final CountDownLatch latchUpdated = new CountDownLatch(1);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchEvicted = new CountDownLatch(1);
        EntryListener listener = new EntryListener() {
            public void entryAdded(EntryEvent entryEvent) {
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent entryEvent) {
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent entryEvent) {
                latchUpdated.countDown();
            }

            public void entryEvicted(EntryEvent entryEvent) {
                latchEvicted.countDown();
            }
        };
        map.addEntryListener(listener, true);
        assertNull(mapSource.put(keyToUpdate, "value5"));
        assertEquals("value5", mapSource.put(keyToUpdate, "value55"));
        assertTrue(mapSource.evict(keyToUpdate));
        assertNull(mapSource.put(keyToUpdate, "value5"));
        assertEquals("value5", mapSource.remove(keyToUpdate));
        assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
        assertTrue(latchUpdated.await(5, TimeUnit.SECONDS));
        assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
        assertTrue(latchEvicted.await(5, TimeUnit.SECONDS));
        map.removeEntryListener(listener);
    }

    @Test(timeout = 60000)
    public void testTcpIpWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        Config c = new XmlConfigBuilder().build();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addAddress(new Address("127.0.0.1", 5701));
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        System.setProperty("hazelcast.build", "2");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        System.setProperty("hazelcast.build", "t");
    }

    @Test(timeout = 60000)
    public void testMulticastWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        System.setProperty("hazelcast.build", "2");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        System.setProperty("hazelcast.build", "t");
    }

    @Test(timeout = 60000)
    public void testMapMaxSize() throws Exception {
        Config c = new XmlConfigBuilder().build();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setEvictionPolicy("LRU");
        mapConfig.setMaxSize(3);
        c.getMapMapConfigs().put("default", mapConfig);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(c);
        IMap map = h.getMap("default");
        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
            int expectedSize = (i < 3) ? i + 1 : 3;
            assertEquals(expectedSize, map.size());
        }
    }

    /**
     * Test for issue #204:
     * http://code.google.com/p/hazelcast/issues/detail?id=204
     * <p/>
     * Summary:
     * Eviction events are not fired
     */
    @Test
    public void testEvictionOfEntriesWithTTL() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map2 = h2.getMap("default");
        TestEntryListener cl1 = new TestEntryListener(100, 0, 0, 100);
        TestEntryListener cl2 = new TestEntryListener(100, 0, 0, 100);
        map1.addEntryListener(cl1, true);
        map2.addEntryListener(cl2, true);
        for (int i = 0; i < 50; i++) {
            map1.put(Integer.valueOf(i), i, 5, TimeUnit.SECONDS);
            map1.put(String.valueOf(i), i, 5, TimeUnit.SECONDS);
        }
        assertTrue(cl1.await(15));
        assertTrue(cl2.await(15));
    }

    @Test(timeout = 180000)
    public void testLosingEntries() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final AtomicBoolean failed = new AtomicBoolean(false);
        new Thread(new Runnable() {
            public void run() {
                try {
                    callSize(h1, 110000);
                } catch (Exception e) {
                    failed.set(true);
                    fail(e.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        }).start();
        Thread.sleep(4000);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        new Thread(new Runnable() {
            public void run() {
                try {
                    callSize(h2, 2000);
                } catch (Exception e) {
                    failed.set(true);
                    fail(e.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        }).start();
        latch.await();
        assertFalse(failed.get());
    }

    private void callSize(HazelcastInstance h, int numberOfIterations) throws Exception {
        Random r = new Random();
        Map<Integer, Integer> map = h.getMap("testMap");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ignored) {
        }
        int size = 0;
        for (int i = 0; i < numberOfIterations; i++) {
            if (i % 1000 == 0) {
                int sizeNow = map.size();
                if (sizeNow < size)
                    throw new RuntimeException("CurrentSize cannot be smaller. " + sizeNow + ", was " + size);
                size = sizeNow;
            }
            map.put(r.nextInt(200000), i);
        }
        h.shutdown();
    }

    @Test(timeout = 60000)
    public void testMapRecovery() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        IMap mm = h.getMap("default");
        mm.put("1", "value");
        assertEquals(1, mm.size());
        assertEquals(1, mm.keySet().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, mm.size());
        assertEquals(1, mm.keySet().size());
        IMap mm2 = h2.getMap("default");
        assertEquals(1, mm2.size());
        assertEquals(1, mm2.keySet().size());
        h.shutdown();
        assertEquals(1, mm2.size());
        assertEquals(1, mm2.keySet().size());
    }

    @Test(timeout = 60000)
    public void testMultiMapRecovery() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        MultiMap mm = h.getMultiMap("default");
        Collection<String> expectedValues = new HashSet<String>();
        expectedValues.add("value1");
        expectedValues.add("value2");
        mm.put("1", "value1");
        mm.put("1", "value2");
        assertEquals(2, mm.size());
        assertEquals(1, mm.keySet().size());
        Collection values = mm.get("1");
        for (Object value : values) {
            assertTrue(expectedValues.contains(value));
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        assertEquals(2, mm.size());
        assertEquals(1, mm.keySet().size());
        values = mm.get("1");
        for (Object value : values) {
            assertTrue(expectedValues.contains(value));
        }
        MultiMap mm2 = h2.getMultiMap("default");
        assertEquals(2, mm2.size());
        assertEquals(1, mm2.keySet().size());
        values = mm2.get("1");
        for (Object value : values) {
            assertTrue(expectedValues.contains(value));
        }
        h.shutdown();
        assertEquals(2, mm2.size());
        assertEquals(1, mm2.keySet().size());
        values = mm2.get("1");
        for (Object value : values) {
            assertTrue(expectedValues.contains(value));
        }
    }

    /**
     * Test case for the issue 144
     * The backup copies were not releasing the locks
     * <p/>
     * Fix: on backup(request)
     * make sure you don't ignore the lock-backup operations where
     * req.backupCount == 0 which is actually an unlock
     */
    @Test(timeout = 60000)
    public void testLockForeverOnBackups() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        ILock lock = h.getLock("FOO");
        lock.lock();
        lock.unlock();
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        final ILock lock2 = h2.getLock("FOO");
        lock2.lock();
        final CountDownLatch latch = new CountDownLatch(2);
        h2.getCluster().addMembershipListener(new
                MembershipListener() {
                    public void memberAdded(MembershipEvent membershipEvent) {
                    }

                    public void memberRemoved(MembershipEvent membershipEvent) {
                        lock2.lock();
                        latch.countDown();
                        lock2.unlock();
                        latch.countDown();
                    }
                }
        );
        lock2.unlock();
        Thread.sleep(1000);
        h.shutdown();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testLockWaiters() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final IMap map1 = h1.getMap("default");
        for (int i = 0; i < 5000; i++) {
            map1.put(i, "value" + i);
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        final IMap map2 = h2.getMap("default");
        testLockWaiters(map1, map2, 1);
        testLockWaiters(map2, map2, 2);
        testLockWaiters(map1, map1, 3);
        testLockWaiters(map2, map1, 4);
    }

    private void testLockWaiters(final IMap mapLocker, final IMap mapWaiter, final Object key) throws Exception {
        mapLocker.lock(key);
        final int count = 10;
        final CountDownLatch latchStart = new CountDownLatch(count);
        final CountDownLatch latchEnd = new CountDownLatch(count);
        ExecutorService es = Executors.newFixedThreadPool(count);
        for (int i = 0; i < count; i++) {
            es.execute(new Runnable() {
                public void run() {
                    latchStart.countDown();
                    mapWaiter.put(key, "value");
                    latchEnd.countDown();
                }
            });
        }
        assertTrue(latchStart.await(1, TimeUnit.SECONDS));
        Thread.sleep(1000); // extra second so that map2.put can actually start
        mapLocker.unlock(key);
        assertTrue(latchEnd.await(10, TimeUnit.SECONDS));
        es.shutdown();
    }

    /**
     * Testing to see if we are able to send and receive
     * 10MB values.
     */
    @Test(timeout = 60000)
    public void testBigValue() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap<Integer, byte[]> map1 = h1.getMap("default");
        IMap<Integer, byte[]> map2 = h2.getMap("default");
        byte[] v = new byte[10000000];
        for (int i = 0; i < v.length; i++) {
            if (i % 10 == 1) {
                v[i] = 109;
            }
        }
        v[v.length - 1] = 109;
        map1.put(1, v);
        assertTrue(Arrays.equals(v, map1.get(1)));
        assertTrue(Arrays.equals(v, map2.get(1)));
    }

    /**
     * Simple symmetric encryption test.
     */
    @Test(timeout = 60000)
    public void testSymmetricEncryption() throws Exception {
        Config c = new XmlConfigBuilder().build();
        SymmetricEncryptionConfig encryptionConfig = new SymmetricEncryptionConfig();
        encryptionConfig.setEnabled(true);
        c.getNetworkConfig().setSymmetricEncryptionConfig(encryptionConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        map1.put(1, "value");
        map1.put(2, new byte[3000]);
        map1.put(3, new byte[1200000]);
        assertEquals(3, map1.size());
        assertEquals(3, map2.size());
        for (int i = 1; i < 4; i++) {
            map2.put(i, i);
        }
        assertEquals(3, map1.size());
        assertEquals(3, map2.size());
    }

    /**
     * Testing if topic can properly listen messages
     * and if topic has any issue after a shutdown.
     */
    @Test
    public void testTopic() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        String topicName = "TestMessages";
        ITopic<String> topic1 = h1.getTopic(topicName);
        final CountDownLatch latch1 = new CountDownLatch(1);
        topic1.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                assertEquals("Test1", msg);
                latch1.countDown();
            }
        });
        ITopic<String> topic2 = h2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                assertEquals("Test1", msg);
                latch2.countDown();
            }
        });
        topic1.publish("Test1");
        h1.shutdown();
        topic2.publish("Test1");
        try {
            assertTrue(latch1.await(5, TimeUnit.SECONDS));
            assertTrue(latch2.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Testing if we are losing any data when we start a node
     * or when we shutdown a node.
     * <p/>
     * Before the shutdowns we are waiting 2 seconds so that we will give
     * remaining members some time to backup their data.
     */
    @Test(timeout = 60000)
    public void testDataRecovery() throws Exception {
        final int size = 1000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map2 = h2.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        IMap map3 = h3.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        IMap map4 = h4.getMap("default");
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        assertEquals(size, map4.size());
        Thread.sleep(2000);
        h4.shutdown();
        assertEquals(size, map1.size());
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        Thread.sleep(2000);
        h1.shutdown();
        assertEquals(size, map2.size());
        assertEquals(size, map3.size());
        Thread.sleep(2000);
        h2.shutdown();
        assertEquals(size, map3.size());
    }

    /**
     * Testing correctness of the sizes during migration.
     * <p/>
     * Migration happens block by block and after completion of
     * each block, next block will start migrating after a fixed
     * time interval. While a block migrating, for the multicall
     * operations, such as size() and queries, there might be a case
     * where data is migrated but not counted/queried. To avoid this
     * hazelcast will compare the block-owners hash. If req.blockId
     * (which is holding requester's block-owners hash value) is not same
     * as the target's block-owners hash value, then request will
     * be re-done.
     */
    @Test(timeout = 240000)
    public void testDataRecoveryAndCorrectness() throws Exception {
        final int size = 1000;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < size; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(size, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map2 = h2.getMap("default");
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        assertEquals(3, h3.getCluster().getMembers().size());
        IMap map3 = h3.getMap("default");
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        IMap map4 = h4.getMap("default");
        for (int i = 0; i < 20000; i++) {
            assertEquals(size, map1.size());
            assertEquals(size, map2.size());
            assertEquals(size, map3.size());
            assertEquals(size, map4.size());
        }
    }

    /**
     * Test case for issue 154
     *
     * @throws Exception
     */
    @Test(timeout = 240000)
    public void testExecutorServiceAndMigration() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        List<DistributedTask> tasks = new ArrayList<DistributedTask>(10000);
        List<ExecutorServiceAndMigrationCallable> callables = new ArrayList<ExecutorServiceAndMigrationCallable>(10000);
        for (int i = 0; i < 10000; i++) {
            ExecutorServiceAndMigrationCallable callable = new ExecutorServiceAndMigrationCallable(i);
            DistributedTask task = new DistributedTask(callable, "T1");
            Hazelcast.getExecutorService().execute(task);
            tasks.add(task);
            callables.add(callable);
        }
        for (int i = 0; i < 10000; i++) {
            ExecutorServiceAndMigrationCallable callable = callables.get(i);
            DistributedTask task = tasks.get(i);
            assertEquals(callable.getInput(), task.get(20, TimeUnit.SECONDS));
        }
    }

    public static class ExecutorServiceAndMigrationCallable implements Callable<Integer>, Serializable {
        int i;

        private ExecutorServiceAndMigrationCallable(int i) {
            this.i = i;
        }

        public int getInput() {
            return i;
        }

        public Integer call() throws Exception {
            IMap<Object, Object> map = Hazelcast.getMap("map1");
            map.lock("T1");
            try {
                ISet<Object> set = Hazelcast.getSet("setName1");
                set.size();
                set.add(i);
                if (i % 10 == 0) {
                    set.remove(i);
                }
            }
            finally {
                map.unlock("T1");
            }
            return i;
        }
    }

    /**
     * Test for issue 157
     */
    @Test(timeout = 16000)
    public void testProxySerialization() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map map = h1.getMap("default");
        map.put("1", "value1");
        IQueue q = h1.getQueue("default");
        q.offer("item1");
        MultiMap mm = h1.getMultiMap("default");
        mm.put("1", "mmValue");
        ILock lock = h1.getLock("serializationTestLock");
        lock.lock();
        ITopic topic = h1.getTopic("default");
        IdGenerator ig = h1.getIdGenerator("default");
        assertEquals(1, ig.newId());
        ISet set = h1.getSet("default");
        set.add("item");
        h2.getMap("amap").put("1", map);
        h2.getMap("amap").put("2", q);
        h2.getMap("amap").put("3", mm);
        h2.getMap("amap").put("4", lock);
        h2.getMap("amap").put("5", topic);
        h2.getMap("amap").put("6", ig);
        h2.getMap("amap").put("7", set);
        Map m1 = (Map) h1.getMap("amap").get("1");
        Map m2 = (Map) h2.getMap("amap").get("1");
        assertEquals("value1", m1.get("1"));
        assertEquals("value1", m2.get("1"));
        IQueue q1 = (IQueue) h1.getMap("amap").get("2");
        IQueue q2 = (IQueue) h2.getMap("amap").get("2");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());
        assertEquals("item1", q2.poll());
        MultiMap mm1 = (MultiMap) h1.getMap("amap").get("3");
        MultiMap mm2 = (MultiMap) h2.getMap("amap").get("3");
        assertTrue(mm1.get("1").contains("mmValue"));
        assertTrue(mm2.get("1").contains("mmValue"));
        ILock lock1 = (ILock) h1.getMap("amap").get("4");
        ILock lock2 = (ILock) h2.getMap("amap").get("4");
        assertEquals("serializationTestLock", lock1.getLockObject());
        assertEquals("serializationTestLock", lock2.getLockObject());
        assertFalse(lock2.tryLock());
        ITopic topic1 = (ITopic) h1.getMap("amap").get("5");
        ITopic topic2 = (ITopic) h2.getMap("amap").get("5");
        final CountDownLatch latch = new CountDownLatch(2);
        topic1.addMessageListener(new MessageListener() {
            public void onMessage(Object msg) {
                assertEquals("message5", msg);
                latch.countDown();
            }
        });
        topic2.addMessageListener(new MessageListener() {
            public void onMessage(Object msg) {
                assertEquals("message5", msg);
                latch.countDown();
            }
        });
        topic.publish("message5");
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        IdGenerator ig1 = (IdGenerator) h1.getMap("amap").get("6");
        IdGenerator ig2 = (IdGenerator) h2.getMap("amap").get("6");
        assertEquals(2, ig1.newId());
        assertEquals(1000001, ig2.newId());
        ISet set1 = (ISet) h1.getMap("amap").get("7");
        ISet set2 = (ISet) h2.getMap("amap").get("7");
        assertTrue(set1.contains("item"));
        assertTrue(set2.contains("item"));
    }

    /**
     * Test for issue 157
     */
    @Test(timeout = 16000)
    public void testMapProxySerializationWhenUsingExecutorService() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map m1 = h1.getMap("default");
        m1.put("1", "value1");
        Future ft = h2.getExecutorService().submit(new DistributedTask(new TestProxyTask(m1), h1.getCluster().getLocalMember()));
        assertEquals("value1", ft.get());
    }

    public static class TestProxyTask implements Callable, Serializable {
        Map map = null;

        public TestProxyTask() {
        }

        public TestProxyTask(Map map) {
            this.map = map;
        }

        public Object call() {
            return map.get("1");
        }
    }

    /**
     * Test for issue 157
     */
    @Test(timeout = 16000)
    public void testHazelcastInstanceSerializationWhenUsingExecutorService() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map m1 = h1.getMap("default");
        m1.put("1", "value1");
        Future ft = h2.getExecutorService().submit(
                new DistributedTask(new TestHazelcastInstanceTask(h1), h1.getCluster().getLocalMember()));
        assertEquals("value1", ft.get());
    }

    public static class TestHazelcastInstanceTask implements Callable, Serializable {
        HazelcastInstance hazelcastInstance = null;

        public TestHazelcastInstanceTask() {
        }

        public TestHazelcastInstanceTask(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        public Object call() {
            return hazelcastInstance.getMap("default").get("1");
        }
    }

    /**
     * Test for issue 157
     */
    @Test(timeout = 16000)
    public void testHazelcastInstanceAwareSerializationWhenUsingExecutorService() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map m1 = h1.getMap("default");
        m1.put("1", "value1");
        Future ft = h2.getExecutorService().submit(
                new DistributedTask(new TestHazelcastInstanceAwareTask(), h1.getCluster().getLocalMember()));
        assertEquals("value1", ft.get());
    }

    public static class TestHazelcastInstanceAwareTask extends HazelcastInstanceAwareObject implements Callable, Serializable {

        public TestHazelcastInstanceAwareTask() {
        }

        public Object call() {
            return hazelcastInstance.getMap("default").get("1");
        }
    }

    @Test
    public void testKeyOwner() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Map<String, String> m1 = h1.getMap("default");
        for (int i = 0; i < 100; i++) {
            m1.put(String.valueOf(i), "value" + i);
        }
        assertNotNull(getKeyOwner(h1, "1"));
    }

    private Member getKeyOwner(HazelcastInstance hi, Object key) throws Exception {
        MultiTask<Member> task = new MultiTask<Member>(new GetOwnerCallable(key), hi.getCluster().getMembers());
        hi.getExecutorService().execute(task);
        Collection<Member> results = task.get();
        for (Member member : results) {
            if (member != null) {
                return member;
            }
        }
        return null;
    }

    public static class GetOwnerCallable extends HazelcastInstanceAwareObject implements Callable<Member>, Serializable {
        private Object key = null;

        public GetOwnerCallable() {
        }

        public GetOwnerCallable(Object key) {
            this.key = key;
        }

        public Member call() throws Exception {
            if (hazelcastInstance.getMap("default").localKeySet().contains(key)) {
                return hazelcastInstance.getCluster().getLocalMember();
            }
            return null;
        }
    }

    @Test
    public void testTopicListenersWithMultiple() throws Exception {
        final CountDownLatch latch = new CountDownLatch(3);
        final MessageListener<Object> ml = new MessageListener<Object>() {
            public void onMessage(Object msg) {
                assertEquals("message1", msg);
                latch.countDown();
            }
        };
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        h1.getTopic("default").addMessageListener(ml);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        h2.getTopic("default").addMessageListener(ml);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        h3.getTopic("default").addMessageListener(ml);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        h4.getTopic("default").publish("message1");
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testTopicListenersWithMultiple2() throws Exception {
        final CountDownLatch latch = new CountDownLatch(4);
        final MessageListener<Object> ml = new MessageListener<Object>() {
            public void onMessage(Object msg) {
                assertEquals("message2", msg);
                latch.countDown();
            }
        };
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getTopic("default2").addMessageListener(ml);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        h2.getTopic("default2").addMessageListener(ml);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        h3.getTopic("default2").addMessageListener(ml);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        h4.getTopic("default2").addMessageListener(ml);
        assertEquals(4, h4.getCluster().getMembers().size());
        h1.getTopic("default2").publish("message2");
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMapListenersWithMultiple() throws Exception {
        final CountDownLatch latch = new CountDownLatch(3);
        final EntryListener<Object, Object> ml = new EntryListener<Object, Object>() {
            public void entryAdded(EntryEvent entryEvent) {
                latch.countDown();
            }

            public void entryRemoved(EntryEvent entryEvent) {
            }

            public void entryUpdated(EntryEvent entryEvent) {
            }

            public void entryEvicted(EntryEvent entryEvent) {
            }
        };
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default3").addEntryListener(ml, true);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        h2.getMap("default3").addEntryListener(ml, true);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        h3.getMap("default3").addEntryListener(ml, true);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        h4.getMap("default3").put("key", "value");
        assertEquals(true, latch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Test for the issue 184
     * <p/>
     * Hazelcas.newHazelcastInstance(new Config()) doesn't join the cluster.
     * new Config() should be enough as the default config.
     */
    @Test(timeout = 240000)
    public void testDefaultConfigCluster() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    /**
     * Fix for the issue 156
     * When an entry is in a map for more than TTL time
     * the entry should get evicted and eviction event
     * should be fired.
     *
     * @throws Exception
     */
    @Test
    public void testMapEvictionWithTTL() throws Exception {
        Config cfg = new Config();
        Map<String, MapConfig> mapConfigs = new HashMap<String, MapConfig>();
        MapConfig mCfg = new MapConfig();
        mCfg.setTimeToLiveSeconds(3);
        mapConfigs.put("testMapEvictionWithTTL", mCfg);
        cfg.setMapMapConfigs(mapConfigs);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(cfg);
        IMap map1 = h1.getMap("testMapEvictionWithTTL");
        final CountDownLatch latch = new CountDownLatch(1);
        map1.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent entryEvent) {
            }

            public void entryRemoved(EntryEvent entryEvent) {
            }

            public void entryUpdated(EntryEvent entryEvent) {
            }

            public void entryEvicted(EntryEvent entryEvent) {
                assertEquals("1", entryEvent.getKey());
                assertEquals("v1", entryEvent.getValue());
                latch.countDown();
            }
        }, true);
        map1.put("1", "v1");
        assertEquals("v1", map1.get("1"));
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertNull(map1.get("1"));
    }

    @Test
    public void testBackupAndMigrations() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        TestMigrationListener listener1 = new TestMigrationListener(5,5);
        h1.getPartitionService().addMigrationListener(listener1);
        int counter = 5000;
        Map<Integer, String> map = new HashMap<Integer, String>();
        for (int i = 0; i < counter; i++) {
            map.put(i, String.valueOf(i));
        }
        IMap map1 = h1.getMap("testIfProperlyBackedUp");
        map1.putAll(map);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        TestMigrationListener listener2 = new TestMigrationListener(5,5);
        h2.getPartitionService().addMigrationListener(listener2);
        IMap map2 = h2.getMap("testIfProperlyBackedUp");
        Thread.sleep(1000);
        for (int i = 0; i < 5; i++) {
            Thread.sleep(10000);
            LocalMapStats mapStats1 = map1.getLocalMapStats();
            LocalMapStats mapStats2 = map2.getLocalMapStats();
            if (mapStats1.getOwnedEntryCount() == counter) {
                Thread.sleep(1000);
            }
            assertEquals(mapStats1.getOwnedEntryCount(), mapStats2.getBackupEntryCount());
            assertEquals("Migrated blocks are not backed up", mapStats2.getOwnedEntryCount(), mapStats1.getBackupEntryCount());
        }
        assertTrue(listener1.await(5));
        assertTrue(listener2.await(5));
    }
}
