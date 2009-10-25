package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.nio.Address;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterTest {

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        Thread.sleep(500);
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
        String mapName = "busyCorIds";
        int ttl = 2;
        Config myConfig = configTTLForMap(mapName, ttl);
        Hazelcast.init(myConfig);
        IMap<String, String> myMap = Hazelcast.getMap(mapName);
        String one = "1";
        myMap.put(one, one);
        String myValue = myMap.get(one);
        Assert.assertTrue(myMap.containsKey(one));
        Thread.sleep((ttl + 1) * 1000);
        myValue = myMap.get(one);
        assertNull(myValue);
        String oneone = "11";
        String existValue = myMap.putIfAbsent(one, oneone);
        assertNull(existValue);
        myValue = myMap.get(one);
        assertEquals(oneone, myValue);
    }

    @Test
    public void testPutIfAbsentWhenThereIsTTLAndRemovedBeforeTTL() throws InterruptedException {
        String mapName = "busyCorIds";
        int ttl = 2;
        Config myConfig = configTTLForMap(mapName, ttl);
        Hazelcast.init(myConfig);
        IMap<String, String> myMap = Hazelcast.getMap(mapName);
        String one = "1";
        myMap.put(one, one);
        String myValue = myMap.get(one);
        assertNotNull(myValue);
        Assert.assertTrue(myMap.containsKey(one));
        myMap.remove(one);
        Thread.sleep((ttl + 1) * 1000);
        myValue = myMap.get(one);
        assertNull(myValue);
        String oneone = "11";
        String existValue = myMap.putIfAbsent(one, oneone);
        assertNull(existValue);
        myValue = myMap.get(one);
        assertEquals(oneone, myValue);
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
        c1.getNetworkConfig().getInterfaces().getLsInterfaces().clear();
        c1.getNetworkConfig().getInterfaces().getLsInterfaces().add("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().setEnabled(true);
        Config c2 = new XmlConfigBuilder().build();
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getInterfaces().getLsInterfaces().clear();
        c2.getNetworkConfig().getInterfaces().getLsInterfaces().add("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().setEnabled(true);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        Hazelcast.shutdownAll();
        c2.setGroupName("differentGroup");
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
    public void testSimpleTcpIp() throws Exception {
        Config c = new XmlConfigBuilder().build();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getJoinMembers().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getJoinMembers().addAddress(new Address("127.1.0.1", 5701));
        c.getNetworkConfig().getInterfaces().getLsInterfaces().add("127.0.0.1");
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(c);
        hNormal.getMap("default").put("1", "first");
        assertEquals("first", hNormal.getMap("default").put("1", "first"));
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
        for (int i = 0; i < 3; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
        }
        assertEquals(3, map.size());
        for (int i = 3; i < 10; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
            assertEquals(3, map.size());
        }
    }

    @Test(timeout = 60000)
    public void testSimpleMulticast() throws Exception {
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(null);
        hNormal.getMap("default").put("1", "first");
        assertEquals("first", hNormal.getMap("default").put("1", "first"));
    }

    @Test(timeout = 120000)
    public void testLosingEntries() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final AtomicBoolean failed = new AtomicBoolean(false);
        new Thread(new Runnable() {
            public void run() {
                try {
                    doIt(h1, 110000);
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
                    doIt(h2, 2000);
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

    private void doIt(HazelcastInstance h, int numberOfIterations) throws Exception {
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
//                System.out.println(h.getName() + " Currect map.size=" + size);
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
        topic1.addMessageListener(new MessageListener() {
            public void onMessage(Object msg) {
                assertEquals("Test1", msg);
                latch1.countDown();
            }
        });
        ITopic<String> topic2 = h2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener() {
            public void onMessage(Object msg) {
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
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(1000, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map2 = h2.getMap("default");
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        IMap map3 = h3.getMap("default");
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        IMap map4 = h4.getMap("default");
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());
        assertEquals(1000, map4.size());
        Thread.sleep(2000);
        h4.shutdown();
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());
        Thread.sleep(2000);
        h1.shutdown();
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());
        Thread.sleep(2000);
        h2.shutdown();
        assertEquals(1000, map3.size());
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
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        IMap map1 = h1.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(1000, map1.size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map2 = h2.getMap("default");
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
        IMap map3 = h3.getMap("default");
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(null);
        IMap map4 = h4.getMap("default");
        for (int i = 0; i < 20000; i++) {
            assertEquals(1000, map1.size());
            assertEquals(1000, map2.size());
            assertEquals(1000, map3.size());
            assertEquals(1000, map4.size());
        }
    }
}