package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.nio.Address;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

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
    public void testMapKeySet() throws Exception {
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
    }

    @Test(timeout = 60000)
    public void testMultiMapKeySet() throws Exception {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        MultiMap mm = h.getMultiMap("default");
        mm.put("1", "value");
        assertEquals(1, mm.size());
        assertEquals(1, mm.keySet().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        assertEquals(1, mm.size());
        assertEquals(1, mm.keySet().size());
        MultiMap mm2 = h2.getMultiMap("default");
        assertEquals(1, mm2.size());
        assertEquals(1, mm2.keySet().size());
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
     * 
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
        for (int i = 0; i < 3; i++) {
            map1.put(i, i);
        }
        assertEquals(3, map1.size());
        assertEquals(3, map2.size());
        for (int i = 0; i < 3; i++) {
            map2.put(i, i);
        }
        assertEquals(3, map1.size());
        assertEquals(3, map2.size());
    }
}