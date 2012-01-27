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

import com.hazelcast.cluster.AddOrRemoveConnection;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.examples.TestApp;
import com.hazelcast.monitor.DistributedMapStatsCallable;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.util.ConcurrentHashSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static com.hazelcast.impl.TestUtil.OrderKey;
import static com.hazelcast.impl.TestUtil.getCMap;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

/**
 * Run these tests with
 * -Xms512m -Xmx512m
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClusterTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void te() throws Exception {
        String classpath = System.getProperty("java.class.path");
        for (String classpathEntry : classpath.split(System.getProperty("path.separator"))) {
            if (classpathEntry.endsWith(".jar")) {
                File jar = new File(classpathEntry);
                JarInputStream is = new JarInputStream(new FileInputStream(jar));
                JarEntry entry;
                while ((entry = is.getNextJarEntry()) != null) {
                    if (entry.getName().endsWith(".class")) {
                        // Class.forName(entry.getName()) and check
                        // for implementation of the interface
                        //                        System.out.println("Class in jar " + entry);
                    }
                }
            } else {
                System.out.println("path " + classpathEntry);
            }
        }
    }

    @Test
    public void testIdle() throws Exception {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setMaxIdleSeconds(3);
        mapConfig.setEvictionDelaySeconds(10);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        Map map = h1.getMap("default");
        map.put(1, 1);
        assertEquals(1, map.get(1));
        sleep(2000);
        assertEquals(1, map.get(1));
        sleep(2000);
        assertEquals(1, map.get(1));
        sleep(4000);
        assertNull(map.get(1));
        assertEquals(0, map.size());
    }

    @Test
    public void testPartitions() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(271, getLocalPartitions(h1).size());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(271, getLocalPartitions(h1).size() + getLocalPartitions(h2).size());
    }

    @Test
    public void testAtomicNumber() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        AtomicNumber a1 = h1.getAtomicNumber("default");
        AtomicNumber a2 = h2.getAtomicNumber("default");
        assertEquals(1, a1.incrementAndGet());
        assertEquals(1, a1.get());
        assertEquals(1, a2.get());
        assertEquals(5, a2.addAndGet(4));
        assertEquals(5, a1.getAndSet(13));
        assertEquals(13, a1.get());
        assertEquals(13, a2.get());
        h1.getLifecycleService().shutdown();
        assertEquals(13, a2.getAndSet(21));
        assertEquals(21, a2.get());
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        AtomicNumber a3 = h3.getAtomicNumber("default");
        assertEquals(20, a3.decrementAndGet());
        assertEquals(20, a2.getAndAdd(-20));
        assertFalse(a2.compareAndSet(1, 6));
        assertFalse(a3.compareAndSet(1, 6));
        assertTrue(a2.compareAndSet(0, 6));
        assertTrue(a3.compareAndSet(6, 0));
        assertEquals(0, a3.get());
        assertEquals(0, a2.get());
        h2.getLifecycleService().shutdown();
        assertEquals(0, a3.get());
    }

    @Test
    public void testFirstNodeNoWait() throws Exception {
        final Config config = new Config();
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
        final Config config = new Config();
        final BlockingQueue<Integer> counts = new ArrayBlockingQueue<Integer>(2);
        final HazelcastInstance[] instances = new HazelcastInstance[2];
        for (int i = 0; i < 2; i++) {
            instances[i] = Hazelcast.newHazelcastInstance(config);
        }
        for (int j = 0; j < 2; j++) {
            final int instanceIndex = j;
            new Thread(new Runnable() {
                public void run() {
                    final HazelcastInstance h = instances[instanceIndex];
                    for (int i = 0; i < 3000; i++) {
                        h.getMap("default").put(i, "value");
                    }
                    counts.offer(getLocalPartitions(h).size());
                }
            }).start();
        }
        int first = counts.take();
        int second = counts.take();
        assertTrue("Found " + first, first > 134);
        assertTrue("Found " + second, second > 134);
        assertEquals(271, second + first);
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

    @Test(timeout = 50000, expected = RuntimeException.class)
    public void testPutAfterShutdown() throws InterruptedException {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        Map map = h1.getMap("default");
        h1.shutdown();
        map.put("1", "value");
    }

    @Test(timeout = 100000)
    public void testSuperClientPartitionOwnership() throws Exception {
        Config configSuperClient = new Config();
        configSuperClient.setLiteMember(true);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(configSuperClient);
        IMap mapSuper = hSuper.getMap("default");
        IMap mapNormal = hNormal.getMap("default");
        for (int i = 0; i < 1000; i++) {
            mapNormal.put("item" + i, "value" + i);
        }
        for (int i = 1000; i < 2000; i++) {
            mapSuper.put("item" + i, "value" + i);
        }
        Set<Partition> partitions2 = hSuper.getPartitionService().getPartitions();
        for (Partition partition : partitions2) {
            assertEquals(partition.getOwner(), hNormal.getCluster().getLocalMember());
        }
        assertEquals(2000, mapNormal.size());
        assertEquals(2000, mapSuper.size());
        assertEquals(0, mapSuper.getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, mapSuper.getLocalMapStats().getBackupEntryCount());
        assertEquals(2000, mapNormal.getLocalMapStats().getOwnedEntryCount());
        assertEquals(0, mapNormal.getLocalMapStats().getBackupEntryCount());
        hNormal.shutdown();
        Thread.sleep(3000);
        Set<Partition> partitions = hSuper.getPartitionService().getPartitions();
        for (Partition partition : partitions) {
            assertNull(partition.getOwner());
        }
        hNormal = Hazelcast.newHazelcastInstance(new Config());
        partitions = hSuper.getPartitionService().getPartitions();
        for (Partition partition : partitions) {
            assertEquals(hNormal.getCluster().getLocalMember(), partition.getOwner());
        }
        assertNull(mapSuper.put("1", "value"));
        hSuper.shutdown();
        partitions = hNormal.getPartitionService().getPartitions();
        for (Partition partition : partitions) {
            assertEquals(hNormal.getCluster().getLocalMember(), partition.getOwner());
        }
        assertEquals("value", hNormal.getMap("default").get("1"));
    }

    @Test(timeout = 50000)
    public void testPutAfterRestart() {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        Map map = h1.getMap("default");
        h1.getLifecycleService().restart();
        map.put("1", "value");
    }

    @Test
    public void testSuperBeingMaster() throws Exception {
        Config config = new Config();
        config.setLiteMember(true);
        final HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance hSuper2 = Hazelcast.newHazelcastInstance(config);
        sleep(11000);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(new Config());
        Map map = hSuper.getMap("default");
        map.put("1", "value");
        assertEquals("value", hNormal.getMap("default").get("1"));
        sleep(10000);
        assertEquals("value", hNormal.getMap("default").get("1"));
        assertEquals("value", map.get("1"));
    }

    @Test(timeout = 30000)
    public void testSuperClientPutAfterBeforeNormalMember() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latchSuperPut = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                Config config = new XmlConfigBuilder().build();
                config.setLiteMember(true);
                final HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(config);
                latch.countDown();
                Map map = hSuper.getMap("default");
                map.put("1", "value");
                latchSuperPut.countDown();
            }
        }).start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
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
                    sleep(1000);
                    h.restart();
                    latch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        interrupter.start();
        map.put("1", "value");
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testRestart2() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map = h2.getMap("default");
        map.put("1", "value1");
        assertEquals(2, h.getCluster().getMembers().size());
        h2.restart();
        sleep(400);
        assertEquals("value1", map.get("1"));
        map.put("1", "value2");
        assertEquals("value2", map.get("1"));
        assertEquals("value2", h.getMap("default").get("1"));
    }

    @Test
    public void issue397MapReplaceLeadsToMemoryLeak() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("def");
        Object old = map1.replace(1, "v");
        assertNull(old);
        old = map1.replace(200, "v");
        assertNull(old);
        old = map1.replace(140, "v");
        assertNull(old);
        old = map1.replace(55, "v");
        assertNull(old);
        assertFalse(map1.containsKey(1));
        assertFalse(map1.containsKey(200));
        assertFalse(map1.containsKey(55));
        assertEquals(0, map1.getLocalMapStats().getBackupEntryCount());
        IMap map2 = h2.getMap("def");
        assertEquals(0, map2.getLocalMapStats().getBackupEntryCount());
    }

    @Test
    public void issue452SetMigration() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        ISet set1 = h1.getSet("mySet");
        for (int i = 0; i < 1000; i++) {
            set1.add(i);
        }
        assertEquals(1000, set1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        ISet set2 = h2.getSet("mySet");
        ISet set3 = h3.getSet("mySet");
        final CountDownLatch latch = new CountDownLatch(1);
        h1.getPartitionService().addMigrationListener(new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                latch.countDown();
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        });
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(1000, set1.size());
        assertEquals(1000, set2.size());
        assertEquals(1000, set3.size());
        h2.getLifecycleService().shutdown();
        assertEquals(1000, set1.size());
        assertEquals(1000, set3.size());
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
            if (instance.getInstanceType().isLock()) {
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
            if (instance.getInstanceType().isLock()) {
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
            if (instance.getInstanceType().isLock()) {
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
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(myConfig);
        IMap<String, String> myMap = hazelcast.getMap(mapName);
        String key = "1";
        String value = "value1";
        myMap.put(key, value);
        assertEquals(value, myMap.get(key));
        assertTrue(myMap.containsKey(key));
        sleep((ttl + 1) * 1000);
        assertFalse(myMap.containsKey(key));
        assertNull(myMap.get(key));
        assertNull(myMap.putIfAbsent(key, "value2"));
    }

    @Test
    public void testPutIfAbsentWhenThereIsTTLAndRemovedBeforeTTL() throws InterruptedException {
        String mapName = "testTTL";
        int ttl = 1;
        Config myConfig = configTTLForMap(mapName, ttl);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(myConfig);
        IMap<String, String> myMap = hazelcast.getMap(mapName);
        String key = "1";
        String value = "value1";
        myMap.put(key, value);
        assertEquals(value, myMap.get(key));
        assertTrue(myMap.containsKey(key));
        assertEquals(value, myMap.remove(key));
        sleep((ttl + 1) * 1000);
        assertFalse(myMap.containsKey(key));
        assertNull(myMap.get(key));
        assertNull(myMap.putIfAbsent(key, "value2"));
    }

    private Config configTTLForMap(String mapName, int ttl) {
        Config myConfig = new Config();
        Map<String, MapConfig> myHazelcastMapConfigs = myConfig.getMapConfigs();
        MapConfig myMapConfig = myHazelcastMapConfigs.get(mapName);
        if (myMapConfig == null) {
            myMapConfig = new MapConfig();
            myMapConfig.setName(mapName);
            myMapConfig.setTimeToLiveSeconds(ttl);
            myConfig.addMapConfig(myMapConfig);
        } else {
            myMapConfig.setTimeToLiveSeconds(ttl);
        }
        return myConfig;
    }

    @Test(timeout = 40000)
    public void testDifferentGroups() {
        Config c1 = new Config();
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().clear();
        c1.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().setEnabled(true);
        Config c2 = new Config();
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().clear();
        c2.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().setEnabled(true);
        c1.getGroupConfig().setName("sameGroup");
        c2.getGroupConfig().setName("sameGroup");
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
        Config c1 = new Config();
        Config c2 = new Config();
        c2.setLiteMember(true);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(c2);
        hNormal.getMap("default").put("1", "first");
        assert hSuper.getMap("default").
                get("1").equals("first");
        hNormal.shutdown();
        hSuper.shutdown();
    }

    @Test(timeout = 60000)
    public void testSuperClientRestart() throws Exception {
        Config configNormal = new Config();
        configNormal.setProperty(GroupProperties.PROP_CONNECTION_MONITOR_INTERVAL, "1");
        configNormal.setProperty(GroupProperties.PROP_CONNECTION_MONITOR_MAX_FAULTS, "1");
        Config configSuper = new Config();
        configSuper.setProperty(GroupProperties.PROP_CONNECTION_MONITOR_INTERVAL, "1");
        configSuper.setProperty(GroupProperties.PROP_CONNECTION_MONITOR_MAX_FAULTS, "1");
        configSuper.setLiteMember(true);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(configNormal);
        HazelcastInstance s = Hazelcast.newHazelcastInstance(configSuper);
        assertEquals(2, h.getCluster().getMembers().size());
        assertEquals(2, s.getCluster().getMembers().size());
        assertFalse(h.getCluster().getLocalMember().isLiteMember());
        assertTrue(s.getCluster().getLocalMember().isLiteMember());
        IMap map = h.getMap("default");
        final IMap maps = s.getMap("default");
        assertNull(map.put("1", "value1"));
        assertEquals("value1", map.get("1"));
        assertEquals("value1", maps.get("1"));
        assertEquals(1, map.size());
        assertEquals(1, maps.size());
        h.shutdown();
        sleep(500);
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
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertEquals(2, h.getCluster().getMembers().size());
        assertEquals(2, s.getCluster().getMembers().size());
        assertFalse(h.getCluster().getLocalMember().isLiteMember());
        assertTrue(s.getCluster().getLocalMember().isLiteMember());
        map = h.getMap("default");
        assertEquals("value3", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        assertEquals(1, maps.size());
    }

    /**
     * 3 node cluster: normal member(h1), super client (hSuper) and another normal member (h2)
     * if h1 goes down, hSuper becomes the oldest member
     * If hSuper fails to update the partition ownerships,
     * h2.getMap("default").get(key) gets into infinite Re-Do.
     *
     * @throws Exception
     */
    @Test
    public void testSuperClientBeingOldestMember() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        Config superConfig = new Config();
        superConfig.setLiteMember(true);
        HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(superConfig);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map = h2.getMap("default");
        h1.getLifecycleService().shutdown();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                assertTrue(map.get("1") == null);
                latch.countDown();
            }
        }).start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testTcpIpWithMembers() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h2, h1);
    }

    @Test(timeout = 60000)
    public void testTcpIp() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addAddress(new Address("127.0.0.1", 5701))
                .addAddress(new Address("127.0.0.1", 5702));
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h2, h1);
    }

    @Test(timeout = 60000)
    public void testTcpIpWithoutInterfaces() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addAddress(new Address("127.0.0.1", 5701))
                .addAddress(new Address("127.0.0.1", 5702));
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h2, h1);
    }

    @Test(timeout = 60000)
    public void testMulticast() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        testTwoNodes(h1, h2);
    }

    private void testTwoNodes(HazelcastInstance h1, HazelcastInstance h2) throws Exception {
        h1.getMultiMap("default").clear();
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
        MultiMap<String, String> mm1 = h1.getMultiMap("default");
        MultiMap<String, String> mm2 = h2.getMultiMap("default");
        mm1.put("Hello", "World");
        Collection<String> values = mm2.get("Hello");
        assertEquals("World", values.iterator().next());
        mm2.put("Hello", "Europe");
        mm1.put("Hello", "America");
        mm1.put("Hello", "Asia");
        mm1.put("Hello", "Africa");
        mm1.put("Hello", "Antartica");
        mm1.put("Hello", "Australia");
        values = mm2.get("Hello");
        assertEquals(7, values.size());
        junit.framework.Assert.assertFalse(mm2.remove("Hello", "Unknown"));
        assertEquals(7, mm1.get("Hello").size());
        assertTrue(mm1.remove("Hello", "Antartica"));
        assertEquals(6, mm1.get("Hello").size());
    }

    @Test(timeout = 120000)
    public void testListeners2() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        final Member member1 = h1.getCluster().getLocalMember();
        final Member member2 = h2.getCluster().getLocalMember();
        final CountDownLatch latchAdded = new CountDownLatch(4);
        final CountDownLatch latchUpdated = new CountDownLatch(2);
        final CountDownLatch latchRemoved = new CountDownLatch(2);
        final CountDownLatch latchEvicted = new CountDownLatch(2);
        EntryListener listener = new EntryListener() {
            public synchronized void entryAdded(EntryEvent entryEvent) {
                if (latchAdded.getCount() > 2) {
                    assertEquals(member1, entryEvent.getMember());
                } else {
                    assertEquals(member2, entryEvent.getMember());
                }
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent entryEvent) {
                assertEquals(member1, entryEvent.getMember());
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent entryEvent) {
                assertEquals(member2, entryEvent.getMember());
                latchUpdated.countDown();
            }

            public void entryEvicted(EntryEvent entryEvent) {
                assertEquals(member2, entryEvent.getMember());
                latchEvicted.countDown();
            }
        };
        IMap map2 = h2.getMap("default");
        IMap map1 = h1.getMap("default");
        Object key = "2133aa";
        map1.addEntryListener(listener, key, true);
        map2.addEntryListener(listener, key, true);
        assertNull(map1.put(key, "value5"));
        assertEquals("value5", map2.put(key, "value55"));
        assertTrue(map2.evict(key));
        assertNull(map2.put(key, "value5"));
        assertEquals("value5", map1.remove(key));
        int waitSeconds = 20;
        assertTrue(latchRemoved.await(waitSeconds, TimeUnit.SECONDS));
        map1.removeEntryListener(listener, key);
        assertFalse(map2.evict(key));
        map2.removeEntryListener(listener, key);
        assertTrue(latchAdded.await(waitSeconds, TimeUnit.SECONDS));
        assertTrue(latchUpdated.await(waitSeconds, TimeUnit.SECONDS));
        assertTrue(latchRemoved.await(waitSeconds, TimeUnit.SECONDS));
        assertTrue(latchEvicted.await(waitSeconds, TimeUnit.SECONDS));
    }

    @Test(timeout = 120000)
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
        int waitSeconds = 20;
        assertTrue(latchAdded.await(waitSeconds, TimeUnit.SECONDS));
        assertTrue(latchUpdated.await(waitSeconds, TimeUnit.SECONDS));
        assertTrue(latchRemoved.await(waitSeconds, TimeUnit.SECONDS));
        assertTrue(latchEvicted.await(waitSeconds, TimeUnit.SECONDS));
        map.removeEntryListener(listener);
    }

    @Test(timeout = 60000)
    public void testTcpIpWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        Config c = new Config();
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
        int maxSize = 40;
        Config c = new Config();
        MapConfig mapConfig = c.getMapConfig("default");
        mapConfig.setEvictionPolicy("LRU");
        mapConfig.setMaxSize(maxSize);
        mapConfig.setEvictionPercentage(25);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(c);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        IMap map = h.getMap("default");
        for (int i = 0; i < 100; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
            int mapSize = map.size();
            assertTrue("CurrentMapSize : " + mapSize, mapSize <= maxSize);
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
        assertTrue(cl1.await(30));
        assertTrue(cl2.await(30));
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
        sleep(4000);
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
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertFalse(failed.get());
    }

    private void callSize(HazelcastInstance h, int numberOfIterations) throws Exception {
        Random r = new Random();
        Map<Integer, Integer> map = h.getMap("testMap");
        try {
            sleep(5000);
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
        sleep(1000);
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
                    long start = System.currentTimeMillis();
                    mapWaiter.put(key, "value");
                    assertTrue((System.currentTimeMillis() - start) >= 1000);
                    latchEnd.countDown();
                }
            });
        }
        assertTrue(latchStart.await(1, TimeUnit.SECONDS));
        sleep(1000); // extra second so that map2.put can actually start
        mapLocker.unlock(key);
        assertTrue(latchEnd.await(10, TimeUnit.SECONDS));
        es.shutdown();
    }

    @Test(timeout = 60000)
    public void testMapLock() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final IMap map1 = h1.getMap("default");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        final IMap map2 = h2.getMap("default");
        testMapLockWaiters(map1, map2, 1);
        testMapLockWaiters(map2, map2, 2);
        testMapLockWaiters(map1, map1, 3);
        testMapLockWaiters(map2, map1, 4);
        assertTrue(map1.lockMap(10, TimeUnit.SECONDS));
        assertNull(map1.put(5, "value5"));
        assertEquals("value5", map1.put(5, "value55"));
        assertEquals("value55", map1.get(5));
        map1.unlockMap();
    }

    private void testMapLockWaiters(final IMap mapLocker, final IMap mapWaiter, final Object key) throws Exception {
        assertTrue(mapLocker.lockMap(10, TimeUnit.SECONDS));
        final int count = 10;
        final CountDownLatch latchStart = new CountDownLatch(count);
        final CountDownLatch latchEnd = new CountDownLatch(count);
        ExecutorService es = Executors.newFixedThreadPool(count);
        for (int i = 0; i < count; i++) {
            es.execute(new Runnable() {
                public void run() {
                    latchStart.countDown();
                    long start = System.currentTimeMillis();
                    mapWaiter.put(key, "value");
                    assertTrue((System.currentTimeMillis() - start) >= 1000);
                    latchEnd.countDown();
                }
            });
        }
        assertTrue(latchStart.await(1, TimeUnit.SECONDS));
        sleep(1000); // extra second so that map2.put can actually start
        mapLocker.unlockMap();
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
            public void onMessage(Message msg) {
                assertEquals("Test1", msg.getMessageObject());
                latch1.countDown();
            }
        });
        ITopic<String> topic2 = h2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals("Test1", msg.getMessageObject());
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

    @Test(timeout = 240000)
    public void testExecutorServiceMultiTask() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        MultiTask multitask = new MultiTask(new DistributedMapStatsCallable("default"), h1.getCluster().getMembers());
        ExecutorService es = h1.getExecutorService();
        for (int i = 0; i < 100; i++) {
            es.execute(multitask);
            Collection results = multitask.get();
            assertEquals("iteration count " + i, 4, results.size());
            for (Object result : results) {
                assertTrue(result instanceof DistributedMapStatsCallable.MemberMapStat);
            }
        }
    }

    /**
     * Test case for issue 154
     *
     * @throws Exception
     */
    @Test(timeout = 240000)
    public void testExecutorServiceAndMigration() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
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
            } finally {
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
        AtomicNumber atomicNumber = h1.getAtomicNumber("default");
        atomicNumber.incrementAndGet();
        atomicNumber.incrementAndGet();
        assertEquals(2, atomicNumber.get());
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
        h2.getMap("amap").put("8", atomicNumber);
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
            public void onMessage(Message msg) {
                assertEquals("message5", msg.getMessageObject());
                latch.countDown();
            }
        });
        topic2.addMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                assertEquals("message5", msg.getMessageObject());
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
        AtomicNumber a1 = (AtomicNumber) h1.getMap("amap").get("8");
        AtomicNumber a2 = (AtomicNumber) h2.getMap("amap").get("8");
        assertEquals(2, a1.get());
        assertEquals(2, a2.get());
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
        Config config = new Config();
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
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

    @Test(timeout = 30000)
    public void testExecutorServiceDeadLock() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        Member target1 = h1.getCluster().getLocalMember();
        Member target2 = h2.getCluster().getLocalMember();
        int executionCount = 20;
        List<DistributedTask<Long>> lsTasks = new ArrayList<DistributedTask<Long>>(executionCount);
        for (int i = 0; i < executionCount; i++) {
            DistributedTask<Long> t1 = new DistributedTask<Long>(new SleepCallable(1000), target1);
            lsTasks.add(t1);
            h2.getExecutorService().execute(t1);
            DistributedTask<Long> t2 = new DistributedTask<Long>(new SleepCallable(2000), target2);
            lsTasks.add(t2);
            h1.getExecutorService().execute(t2);
        }
        sleep(7000);
        for (DistributedTask<Long> task : lsTasks) {
            Long result = task.get(1, TimeUnit.SECONDS);
            assertTrue(result == 1000 || result == 2000);
        }
    }

    /**
     * Test for issue 157
     */
    @Test(timeout = 16000)
    public void testHazelcastInstanceAwareSerializationWhenUsingExecutorService() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
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
            public void onMessage(Message msg) {
                assertEquals("message1", msg.getMessageObject());
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
            public void onMessage(Message msg) {
                assertEquals("message2", msg.getMessageObject());
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
        cfg.setMapConfigs(mapConfigs);
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

    @Test(timeout = 25000, expected = MemberLeftException.class)
    public void testExecutorWhenOneMemberDiesWhileExecuting() throws ExecutionException, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Set<Member> members = h2.getCluster().getMembers();
        MultiTask<Long> task = new MultiTask<Long>(new SleepCallable(10000), members);
        h2.getExecutorService().execute(task);
        sleep(2000);
        h1.shutdown();
        task.get();
    }

    /**
     * Test case for issue 265.
     * Lock should invalidate the locally owned cache.
     */
    @Test
    public void testConcurrentLockPrimitive() throws Exception {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(new Config());
        final int threads = 10;
        final IMap<Object, Object> testMap = instance.getMap("testConcurrentLockPrimitive");
        assertNull(testMap.putIfAbsent(1L, 0L));
        assertEquals(0L, testMap.get(1L));
        final AtomicLong count = new AtomicLong(0);
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final int total = 50000;
        final CountDownLatch countDownLatch = new CountDownLatch(total);
        final Set<Long> values = new ConcurrentHashSet<Long>();
        for (int i = 0; i < threads; i++) {
            pool.execute(new Runnable() {
                public void run() {
                    while (count.incrementAndGet() < total + 1) {
                        Long v = (Long) testMap.get(1L);
                        assertNotNull(v);
                        testMap.lock(1L);
                        try {
                            Long value = (Long) testMap.get(1L);
                            if (!values.add(value)) {
                                fail(value + " already exist!");
                            }
                            assertNotNull(value);
                            testMap.put(1L, value + 1);
                        } finally {
                            testMap.unlock(1L);
                            countDownLatch.countDown();
                        }
                    }
                }
            });
        }
        countDownLatch.await();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        Long value = (Long) testMap.get(1L);
        assertEquals(Long.valueOf(total), value);
    }

    @Test
    public void testMemberFiredTheEventIsLocal() throws InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        IMap map = h1.getMap("default");
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent entryEvent) {
                if (entryEvent.getMember().localMember()) {
                    latch.countDown();
                }
            }

            public void entryRemoved(EntryEvent entryEvent) {
            }

            public void entryUpdated(EntryEvent entryEvent) {
            }

            public void entryEvicted(EntryEvent entryEvent) {
            }
        }, false);
        map.put(1, 1);
        boolean isLocal = latch.await(3, TimeUnit.SECONDS);
        assertTrue("localMember() on member that fired event should return true, but was false", isLocal);
    }

    @Test(expected = MemberLeftException.class)
    public void distributedTaskShouldThrowMemberLeftExceptionWhenTargetMemberRemoved() throws ExecutionException, TimeoutException, InterruptedException {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        ExecutorService ex = h2.getExecutorService();
        FutureTask<String> ft = new DistributedTask<String>(new TestApp.Echo("hello"), h1.getCluster().getLocalMember());
        h1.shutdown();
        ex.submit(ft);
        ft.get();
    }

    @Test
    public void issue370() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Queue<String> q1 = h1.getQueue("q");
        Queue<String> q2 = h2.getQueue("q");
        for (int i = 0; i < 5; i++) {
            q1.offer("item" + i);
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());
        assertEquals("item0", q2.poll());
        assertEquals("item1", q2.poll());
        assertEquals("item2", q2.poll());
        Thread.sleep(1000);
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        h1.shutdown();
        Thread.sleep(1000);
        assertEquals(2, q2.size());
        h1 = Hazelcast.newHazelcastInstance(null);
        q1 = h1.getQueue("q");
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        Thread.sleep(1000);
        h2.shutdown();
        Thread.sleep(1000);
        assertEquals(2, q1.size());
    }

    @Test
    public void issue391() throws Exception {
        // passed
        final Collection<String> results = new CopyOnWriteArrayList<String>();
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(null);
        final CountDownLatch latchOffer = new CountDownLatch(1);
        final CountDownLatch latchTake = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < 5; i++) {
                        results.add((String) hz1.getQueue("q").take());
                    }
                    latchTake.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 5; i++) {
                    hz2.getQueue("q").offer(Integer.toString(i));
                }
                latchOffer.countDown();
            }
        }).start();
        Assert.assertTrue(latchOffer.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(latchTake.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(hz1.getQueue("q").isEmpty());
        hz1.getLifecycleService().shutdown();
        Assert.assertTrue(hz2.getQueue("q").isEmpty());
        assertArrayEquals(new Object[]{"0", "1", "2", "3", "4"}, results.toArray());
    }

    @Test(timeout = 100000)
    public void testSplitBrainMulticast() throws Exception {
        splitBrain(true);
    }

    @Test(timeout = 100000)
    public void testSplitBrainTCP() throws Exception {
        splitBrain(false);
    }

    public void splitBrain(boolean multicast) throws Exception {
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
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);
        int size = 500;
        for (int i = 0; i < size; i++) {
            h2.getMap("default").put(i, "value" + i);
            h2.getMultiMap("default").put(i, "value" + i);
            h2.getMultiMap("default").put(i, "value0" + i);
        }
        for (int i = 100; i < size + 100; i++) {
            h1.getMap("default").put(i, "value" + i);
            h1.getMultiMap("default").put(i, "value" + i);
            h1.getMultiMap("default").put(i, "value0" + i);
        }
        assertEquals(size, h2.getMap("default").size());
        assertEquals(2 * size, h2.getMultiMap("default").size());
        assertEquals(size, h1.getMap("default").size());
        assertEquals(2 * size, h1.getMultiMap("default").size());
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        Thread.sleep(2000);
        c1.getGroupConfig().setName("sameGroup");
        assertTrue(l.waitFor(LifecycleEvent.LifecycleState.RESTARTED, 40));
        assertEquals(1, l.getCount(LifecycleEvent.LifecycleState.RESTARTING));
        assertEquals(1, l.getCount(LifecycleEvent.LifecycleState.RESTARTED));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        int newMapSize = size + 100;
        int newMultiMapSize = 2 * newMapSize;
        assertEquals(newMapSize, h1.getMap("default").size());
        assertEquals(newMapSize, h2.getMap("default").size());
        assertEquals(newMultiMapSize, h2.getMultiMap("default").size());
        assertEquals(newMultiMapSize, h1.getMultiMap("default").size());
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
                    long now = System.currentTimeMillis();
                    received = eventQueue.poll(remainingMillis, TimeUnit.MILLISECONDS);
                    remainingMillis -= (System.currentTimeMillis() - now);
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

    @Test(timeout = 100000)
    public void testTwoNodesSameTimeLockMap() throws InterruptedException {
        final String MAP_NAME = "testmap";
        final String KEY = "testkey";
        final CountDownLatch latch = new CountDownLatch(10000);
        final CountDownLatch start = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            new Thread(new Runnable() {
                public void run() {
                    HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
                    IMap<String, Integer> map = h.getMap(MAP_NAME);
                    start.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    while (latch.getCount() > 0) {
                        if (!map.lockMap(1, TimeUnit.SECONDS)) {
                            continue;
                        }
                        try {
                            Integer value = map.get(KEY);
                            if (value == null)
                                value = 0;
                            map.put(KEY, value + 1);
                        } finally {
                            map.unlockMap();
                            latch.countDown();
                        }
                    }
                    h.getLifecycleService().shutdown();
                }
            }).start();
        }
        assertTrue(latch.await(200, TimeUnit.SECONDS));
    }

    @Test
    public void secondLockOnMapShouldReturnFalse() {
        HazelcastInstance hzi1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance hzi2 = Hazelcast.newHazelcastInstance(null);
        IMap<Object, Object> map1 = hzi1.getMap("dummymap");
        IMap<Object, Object> map2 = hzi2.getMap("dummymap");
        boolean lockMap1 = map1.lockMap(0,
                TimeUnit.SECONDS);           // A
        assertTrue(lockMap1);
        boolean lockMap2 = map2.lockMap(1,
                TimeUnit.SECONDS);           // B
        assertFalse(lockMap2);
        hzi1.getLifecycleService().shutdown();
        hzi2.getLifecycleService().shutdown();
    }

    @Test
    public void secondLockOnMapShouldReturnTrueWhenFirstLockReleased() throws InterruptedException {
        final HazelcastInstance hzi1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hzi2 = Hazelcast.newHazelcastInstance(null);
        final IMap<Object, Object> map1 = hzi1.getMap("dummymap");
        final IMap<Object, Object> map2 = hzi2.getMap("dummymap");
        final CountDownLatch acquireLock = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                map1.lockMap(0, TimeUnit.SECONDS);
                acquireLock.countDown();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                map1.unlockMap();
            }
        }).start();
        assertTrue(acquireLock.await(20, TimeUnit.SECONDS));
        boolean lockMap2 = map2.lockMap(10, TimeUnit.SECONDS);
        assertTrue(lockMap2);
        hzi1.getLifecycleService().shutdown();
        hzi2.getLifecycleService().shutdown();
    }

    @Test
    public void mapLock() throws InterruptedException {
        final HazelcastInstance hzi1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance hzi2 = Hazelcast.newHazelcastInstance(null);
        final IMap<Object, Object> map1 = hzi1.getMap("dummymap");
        final IMap<Object, Object> map2 = hzi2.getMap("dummymap");
        final CountDownLatch acquireLock = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                map1.lockMap(0, TimeUnit.SECONDS);
                acquireLock.countDown();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                map1.put(1, 1);
                map1.unlockMap();
            }
        }).start();
        assertTrue(acquireLock.await(20, TimeUnit.SECONDS));
        map2.put(2, 2);
        boolean lockMap2 = map2.lockMap(10, TimeUnit.SECONDS);
        assertTrue(lockMap2);
        hzi1.getLifecycleService().shutdown();
        hzi2.getLifecycleService().shutdown();
    }

    @Test
    public void unresolvableHostName() {
        Config config = new Config();
        config.getGroupConfig().setName("abc");
        config.getGroupConfig().setPassword("def");
        Join join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().setMembers(Arrays.asList(new String[]{"localhost", "nonexistinghost"}));
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        assertEquals(1, hz.getCluster().getMembers().size());
    }

    @Test
    public void mPutAllAndTransaction() {
        IMap<Integer, Integer> map = Hazelcast.getMap("def");
        Hazelcast.getTransaction().begin();
        Map<Integer, Integer> localMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < 10; i++) {
            localMap.put(i, i);
        }
        map.putAll(localMap);
        Hazelcast.getTransaction().rollback();
        assertEquals(0, map.size());
    }

    @Test
    public void testShutdownAllAfterIncompleteTransaction() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        IMap map1 = h1.getMap("default");
        Transaction txn = h1.getTransaction();
        txn.begin();
        map1.put("1", "value1");
        Hazelcast.shutdownAll();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        IMap map2 = h2.getMap("default");
        assertNull(map2.get("1"));
    }

    @Test
    public void multimapShouldNotBeAffectedByDefaultMapConfig() {
        Config config = new XmlConfigBuilder().build();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(0);
        mapStoreConfig.setClassName("com.hazelcast.examples.DummyStore");
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        MultiMap<Object, Object> mmap = hazelcast.getMultiMap("testmultimap");
        mmap.put("foo", "1");
        mmap.put("foo", "2");
        mmap.get("foo").size();
    }

    @Test
    public void issue427QOfferIncorrectWithinTransaction() {
        Config config = new Config();
        config.getQueueConfig("default").setBackingMapRef("default").setMaxSizePerJVM(100);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        h.getTransaction().begin();
        IQueue q = h.getQueue("default");
        for (int i = 0; i < 100; i++) {
            q.offer(i);
        }
        boolean result = q.offer(101);
        assertEquals(100, q.size());
        assertFalse(result);
        h.getLifecycleService().shutdown();
    }

    @Test
    public void testMapEvictWithTTLAndListener() throws InterruptedException {
        int count = 100;
        final CountDownLatch latch = new CountDownLatch(count);
        IMap<String, String> map = Hazelcast.newHazelcastInstance(null).getMap("testMapEvictAndListener");
        map.addEntryListener(new EntryListener<String, String>() {
            public void entryAdded(EntryEvent<String, String> stringStringEntryEvent) {
            }

            public void entryRemoved(EntryEvent<String, String> stringStringEntryEvent) {
            }

            public void entryUpdated(EntryEvent<String, String> stringStringEntryEvent) {
            }

            public void entryEvicted(EntryEvent<String, String> stringStringEntryEvent) {
                latch.countDown();
                assertNotNull("The Value is null", stringStringEntryEvent.getValue());
            }
        }, true);
        for (int i = 0; i < count; i++) {
            map.put("key", "" + i, 3, TimeUnit.MILLISECONDS);
            Thread.sleep(15);
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS));
    }

    @Test
    public void testGetAll() {
        Set<String> keys = new HashSet<String>(1000);
        for (int i = 0; i < 1000; i++) {
            keys.add(String.valueOf(i));
        }
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap<String, Object> map3 = h3.getMap("default");
        assertNull(map3.get("1"));
        map3.getAll(keys);
    }

    @Test
    public void testPutAll() {
        Hazelcast.newHazelcastInstance(new Config());
        Hazelcast.newHazelcastInstance(new Config());
        Map<Integer, Integer> entries = new HashMap<Integer, Integer>(1000);
        for (int i = 0; i < 1000; i++) {
            entries.put(i, i);
        }
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        IMap<Integer, Integer> map = h3.getMap("default");
        map.putAll(entries);
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, map.get(i).intValue());
        }
    }

    @Test
    public void testMapAndMultiMapDestroyWithNearCache() throws Exception {
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setMaxSize(1000);
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        map1.put("1", "value");
        // call map.get() to make sure they are near cached.
        for (int i = 0; i < 2; i++) {
            map1.get("1");
            map2.get("1");
        }
        map1.destroy();
        Thread.sleep(1000);
        assertNull(map2.get("1"));
        assertNull(h2.getMap("default").get("1"));
        assertFalse(map2.containsKey("1"));
        assertFalse(h2.getMap("default").containsKey("1"));
        assertEquals(0, map2.size());
        assertEquals(0, map2.entrySet().size());
        assertNull(map1.get("1"));
        assertFalse(map1.containsKey("1"));
        assertEquals(0, map1.size());
        assertEquals(0, map1.entrySet().size());
        // now test multimap destroy
        final MultiMap<Integer, Integer> m1 = h1.getMultiMap("default");
        final MultiMap<Integer, Integer> m2 = h2.getMultiMap("default");
        for (int i = 0; i < 999; i++) {
            m1.put(i, i);
            for (int a = 0; a < 2; a++) {
                m1.get(i);
                m2.get(i);
            }
        }
        assertTrue(m1.containsKey(1));
        assertTrue(m2.containsKey(1));
        m1.destroy();
        Thread.sleep(1000);
        String longName = Prefix.MULTIMAP + "default";
        assertFalse(TestUtil.getNode(h1).factory.proxies.containsKey(new FactoryImpl.ProxyKey(longName, null)));
        assertFalse(TestUtil.getNode(h1).factory.proxiesByName.containsKey(longName));
        assertFalse(TestUtil.getNode(h2).factory.proxies.containsKey(new FactoryImpl.ProxyKey(longName, null)));
        assertFalse(TestUtil.getNode(h2).factory.proxiesByName.containsKey(longName));
        assertNull(getCMap(h1, longName));
        assertNull(getCMap(h2, longName));
        assertFalse(m1.containsKey(1));
        assertFalse(m2.containsKey(1));
    }

    @Test
    public void testQueueDestroy() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 999; i++) {
            q2.offer(i);
        }
        assertEquals(999, q1.size());
        assertEquals(999, q2.size());
        q1.destroy();
        Thread.sleep(1000);
        String queueLongName = Prefix.QUEUE + "default";
        String mapLongName = Prefix.MAP + Prefix.QUEUE + "default";
        assertFalse(TestUtil.getNode(h1).factory.proxies.containsKey(new FactoryImpl.ProxyKey(queueLongName, null)));
        assertFalse(TestUtil.getNode(h1).factory.proxiesByName.containsKey(queueLongName));
        assertFalse(TestUtil.getNode(h2).factory.proxies.containsKey(new FactoryImpl.ProxyKey(queueLongName, null)));
        assertFalse(TestUtil.getNode(h2).factory.proxiesByName.containsKey(queueLongName));
        assertFalse(TestUtil.getNode(h1).factory.proxies.containsKey(new FactoryImpl.ProxyKey(mapLongName, null)));
        assertFalse(TestUtil.getNode(h1).factory.proxiesByName.containsKey(mapLongName));
        assertFalse(TestUtil.getNode(h2).factory.proxies.containsKey(new FactoryImpl.ProxyKey(mapLongName, null)));
        assertFalse(TestUtil.getNode(h2).factory.proxiesByName.containsKey(mapLongName));
        assertNull(getCMap(h1, mapLongName));
        assertNull(getCMap(h2, mapLongName));
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());
    }

    @Test
    public void testTxn() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        IMap<Object, Object> h1map = h1.getMap("TestMap");
        MultiMap<Object, Object> h1multimap = h1.getMultiMap("multiMap");
        IMap<Object, Object> h2map = h2.getMap("TestMap");
        MultiMap<Object, Object> h2multimap = h2.getMultiMap("multiMap");
        Transaction txn = h1.getTransaction();
        txn.begin();
        h1map.put("somekey", "somevalue");
        h1multimap.put("h1", "somekey");
        txn.commit();
        assertTrue(h1map.containsKey("somekey"));
    }

    @Test
    public void testSplitBrain() throws InterruptedException {
        Config config = new Config();
        config.getGroupConfig().setName("split");
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);
        Thread.sleep(1000);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());
        Thread.sleep(10000);
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
        Hazelcast.shutdownAll();
    }

    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null) return;
        if (h2 == null) return;
        final FactoryImpl f1 = (FactoryImpl) ((FactoryImpl.HazelcastInstanceProxy) h1).getHazelcastInstance();
        final FactoryImpl f2 = (FactoryImpl) ((FactoryImpl.HazelcastInstanceProxy) h2).getHazelcastInstance();
        AddOrRemoveConnection addOrRemoveConnection1 = new AddOrRemoveConnection(f2.node.address, false);
        addOrRemoveConnection1.setNode(f1.node);
        f1.node.clusterManager.enqueueAndWait(addOrRemoveConnection1, 5);
        AddOrRemoveConnection addOrRemoveConnection2 = new AddOrRemoveConnection(f1.node.address, false);
        addOrRemoveConnection2.setNode(f2.node);
        f2.node.clusterManager.enqueueAndWait(addOrRemoveConnection2, 5);
    }

    @Test
    public void testMapPutAndGetUseBackupData() throws Exception {
        Config config = new XmlConfigBuilder().build();
        String mapName1 = "testMapPutAndGetUseBackupData";
        String mapName2 = "testMapPutAndGetUseBackupData2";
        MapConfig mapConfig1 = new MapConfig();
        mapConfig1.setName(mapName1);
        mapConfig1.setReadBackupData(true);
        MapConfig mapConfig2 = new MapConfig();
        mapConfig2.setName(mapName2);
        mapConfig2.setReadBackupData(false);
        config.addMapConfig(mapConfig1);
        config.addMapConfig(mapConfig2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> m1 = h1.getMap(mapName1);
        IMap<Object, Object> m2 = h1.getMap(mapName2);
        m1.put(1, 1);
        m2.put(1, 1);
        assertEquals(1, m1.get(1));
        assertEquals(1, m1.get(1));
        assertEquals(1, m1.get(1));
        assertEquals(1, m2.get(1));
        assertEquals(1, m2.get(1));
        assertEquals(1, m2.get(1));
        assertEquals(3, m1.getLocalMapStats().getHits());
        assertEquals(3, m2.getLocalMapStats().getHits());
    }

    @Test
    public void testLockKeyWithUseBackupData() {
        Config config = new XmlConfigBuilder().build();
        String mapName1 = "testLockKeyWithUseBackupData";
        MapConfig mapConfig1 = new MapConfig();
        mapConfig1.setName(mapName1);
        mapConfig1.setReadBackupData(true);
        config.addMapConfig(mapConfig1);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<String, String> map = h1.getMap(mapName1);
        map.lock("Hello");
        try {
            junit.framework.Assert.assertFalse(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
        map.put("Hello", "World");
        map.lock("Hello");
        try {
            assertTrue(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
        map.remove("Hello");
        map.lock("Hello");
        try {
            junit.framework.Assert.assertFalse(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
    }

    @Test
    public void testIssue290() throws Exception {
        String mapName = "testIssue290";
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setTimeToLiveSeconds(1);
        config.addMapConfig(mapConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> m1 = h1.getMap(mapName);
        m1.put(1, 1);
        assertEquals(1, m1.get(1));
        assertEquals(1, m1.get(1));
        Thread.sleep(1050);
        assertEquals(null, m1.get(1));
        m1.put(1, 1);
        assertEquals(1, m1.get(1));
    }

    @Test
    public void testMultiMapTransactions() throws Exception {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(new Config());
        Hazelcast.newHazelcastInstance(new Config());
        MultiMap<String, String> multimap = hazelcast.getMultiMap("def");
        final Transaction transaction = hazelcast.getTransaction();
        transaction.begin();
        for (int i = 0; (i < 1000); ++i) {
            final String element = Integer.toString(i);
            multimap.put(element, element);
        }
        transaction.commit();
        Thread.sleep(2000);
        assertEquals(1000, multimap.size());
    }

    @Test
    public void testAffinity() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        final IMap m1 = h1.getMap("default");
        final IMap m2 = h2.getMap("default");
        int count = 10000;
        OrderKey[] keys = new OrderKey[count];
        for (int i = 0; i < count; i++) {
            OrderKey key = new OrderKey(i, i % 119);
            keys[i] = key;
            m1.put(key, i);
        }
        Collection<Future<Boolean>> callableFutures = new LinkedList<Future<Boolean>>();
        Collection<Future> runnableFutures = new LinkedList<Future>();
        ExecutorService es3 = h3.getExecutorService();
        for (OrderKey key : keys) {
            Member member1 = h1.getPartitionService().getPartition(key).getOwner();
            Member member2 = h1.getPartitionService().getPartition(key.getPartitionKey()).getOwner();
            junit.framework.Assert.assertEquals(member1, member2);
            junit.framework.Assert.assertEquals(key.getOrderId(), m1.get(key));
            callableFutures.add(es3.submit(new TestUtil.OrderUpdateCallable(key.getOrderId(), key.getCustomerId())));
            runnableFutures.add(es3.submit(new TestUtil.OrderUpdateRunnable(key.getOrderId(), key.getCustomerId())));
        }
        for (Future future : callableFutures) {
            assertTrue((Boolean) future.get(10, TimeUnit.SECONDS));
        }
        for (Future future : runnableFutures) {
            future.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTwoMemberTransactionIsolation() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map = h1.getMap("default");
        Config c1 = new Config();
        c1.getGroupConfig().setName("differentGroup");
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c1);
        final IQueue q = h2.getQueue("abc");
        q.offer("item1");
        q.offer("item2");
        Transaction t = h2.getTransaction();
        t.begin();
        Object o = q.take();
        junit.framework.Assert.assertEquals("item1", o);
        map.put("1", "value");
        t.commit();
        final CountDownLatch l = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                Transaction t = h2.getTransaction();
                t.begin();
                try {
                    Object o = q.take();
                    junit.framework.Assert.assertEquals("item2", o);
                    map.put("1", "value2");
                    t.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                }
                l.countDown();
            }
        }).start();
        assertTrue(l.await(5, TimeUnit.SECONDS));
        junit.framework.Assert.assertEquals(1, map.size());
        junit.framework.Assert.assertEquals("value2", map.get("1"));
    }

    @Test
    public void testLoadFromStore() {
        final String MAP_NAME = "testMap";
        final ConcurrentMap<Integer, String> STORE =
                new ConcurrentHashMap<Integer, String>();
        STORE.put(1, "one");
        STORE.put(102, "two");
        STORE.put(3, "three");
        STORE.put(104, "four");
        STORE.put(5, "five");
        STORE.put(106, "six");
        STORE.put(7, "seven");
        STORE.put(108, "eight");
        STORE.put(9, "nine");
        STORE.put(110, "ten");
        STORE.put(11, "eleven");
        STORE.put(112, "twelve");
        Config config = new Config();
        config
                .getMapConfig(MAP_NAME)
                .setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1)
                        .setImplementation(new MapStore<Integer, String>() {
                            public String load(Integer key) {
                                String value = STORE.get(key);
                                return value;
                            }

                            public Map<Integer, String> loadAll(Collection<Integer> keys) {
                                Map<Integer, String> result = new HashMap<Integer, String>();
                                for (Integer key : keys) {
                                    String value = load(key);
                                    if (value != null) {
                                        result.put(key, value);
                                    }
                                }
                                return result;
                            }

                            public Set<Integer> loadAllKeys() {
                                return STORE.keySet();
                            }

                            public void store(Integer key, String value) {
                                STORE.put(key, value);
                            }

                            public void storeAll(Map<Integer, String> map) {
                                for (Map.Entry<Integer, String> entry : map.entrySet()) {
                                    store(entry.getKey(), entry.getValue());
                                }
                            }

                            public void delete(Integer key) {
                                STORE.remove(key);
                            }

                            public void deleteAll(Collection<Integer> keys) {
                                for (Integer key : STORE.keySet()) {
                                    delete(key);
                                }
                            }
                        }));
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.newHazelcastInstance(config);
        IMap<Integer, String> m1 = hc1.getMap(MAP_NAME);
        IMap<Integer, String> m2 = hc2.getMap(MAP_NAME);
        junit.framework.Assert.assertEquals(STORE.size(), m1.keySet().size());
        junit.framework.Assert.assertEquals(STORE.size(), m2.keySet().size());
        hc1.getLifecycleService().shutdown();
        junit.framework.Assert.assertEquals(STORE.size(), m2.keySet().size());
    }

    @Test
    public void testNewInstanceByName() {
        Config config = new Config();
        config.setInstanceName("test");
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.getHazelcastInstanceByName("test");
        HazelcastInstance hc3 = Hazelcast.getHazelcastInstanceByName(hc1.getName());
        assertTrue(hc1 == hc2);
        assertTrue(hc1 == hc3);
        hc1.getLifecycleService().shutdown();
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void testNewInstanceByNameFail() {
        Config config = new Config();
        config.setInstanceName("test");
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testIssue767ItemListenerUnderTransaction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(3);
        final ItemListener listener = new ItemListener() {
            public void itemAdded(ItemEvent item) {
                latch.countDown();
                System.out.println(item);
            }

            public void itemRemoved(ItemEvent item) {
            }
        };
        class TestTask {
            public void test(HazelcastInstance hz, Object value) {
                ISet set = hz.getSet("test");
                set.addItemListener(listener, true);
                Transaction tx = hz.getTransaction();
                tx.begin();
                set.add(value);
                tx.commit();
            }
        }
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(null);
        new TestTask().test(hz1, "test1");
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        new TestTask().test(hz2, "test2");
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }
}
