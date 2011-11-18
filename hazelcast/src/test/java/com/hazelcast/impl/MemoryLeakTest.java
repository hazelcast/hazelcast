package com.hazelcast.impl;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MemoryLeakTest {

    @Test
    public void testShutdownAllMemoryLeak() throws Exception {
        Runtime.getRuntime().gc();
        long usedMemoryInit = getUsedMemoryAsMB();
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance[] instances = new HazelcastInstance[4];
        instances[0] = h1;
        instances[1] = h2;
        instances[2] = h3;
        instances[3] = h4;
        IMap map1 = h1.getMap("default");
        final int size = 10000;
        for (int i = 0; i < size; i++) {
            map1.put(i, new byte[10000]);
        }
        final ExecutorService es = Executors.newFixedThreadPool(4);
        final CountDownLatch latch = new CountDownLatch(4);
        for (int a = 0; a < 4; a++) {
            final int t = a;
            es.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < size; i++) {
                        instances[t].getMap("default").get(i);
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        es.shutdown();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        Hazelcast.shutdownAll();
        waitForGC(10 + usedMemoryInit, 100);
    }
    
	@Test
	public void testTTLAndMemoryLeak() throws Exception {
		Runtime.getRuntime().gc();
		long usedMemoryInit = getUsedMemoryAsMB();
		Config config = new Config();
		MapConfig mapConfig = config.getMapConfig("default");
		mapConfig.setTimeToLiveSeconds(15);
		final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance[] instances = new HazelcastInstance[4];
		instances[0] = h1;
		instances[1] = h2;
		instances[2] = h3;
		instances[3] = h4;
		IMap map1 = h1.getMap("default");
		final int size = 10000;
		for (int i = 0; i < size; i++) {
			map1.put(i, new byte[10000]);
		}
		long usedMemoryStart = getUsedMemoryAsMB();
		assertTrue("UsedMemoryStart: " + usedMemoryStart, usedMemoryStart > 200);
		final ExecutorService es = Executors.newFixedThreadPool(4);
		final CountDownLatch latch = new CountDownLatch(4);
		for (int a = 0; a < 4; a++) {
			final int t = a;
			es.execute(new Runnable() {
				public void run() {
					for (int i = 0; i < size; i++) {
						instances[t].getMap("default").get(i);
					}
					latch.countDown();
				}
			});
		}
		latch.await();
		es.shutdown();
		assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
		waitForGC(25 + usedMemoryInit, 100);
	}

	@Ignore
	private void waitForGC(long limit, int maxSeconds) throws InterruptedException {
		if (getUsedMemoryAsMB() < limit) {
			return;
		}
		for (int i = 0; i < maxSeconds; i++) {
			sleep(1000);
			Runtime.getRuntime().gc();
			if (getUsedMemoryAsMB() < limit) {
				return;
			}
		}
		fail(String.format("UsedMemory now: %s but expected max: %s", getUsedMemoryAsMB(), limit));
	}

	@Test
	public void testTTLAndMemoryLeak2() throws Exception {
		Runtime.getRuntime().gc();
		long usedMemoryInit = getUsedMemoryAsMB();
		Config config = new Config();
		final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance[] instances = new HazelcastInstance[4];
		instances[0] = h1;
		instances[1] = h2;
		instances[2] = h3;
		instances[3] = h4;
		IMap map1 = h1.getMap("default");
		final int size = 10000;
		for (int i = 0; i < size; i++) {
			map1.put(i, new byte[10000], 15, TimeUnit.SECONDS);
		}
		long usedMemoryStart = getUsedMemoryAsMB();
		assertTrue("UsedMemoryStart: " + usedMemoryStart, usedMemoryStart > 200);
		final ExecutorService es = Executors.newFixedThreadPool(4);
		final CountDownLatch latch = new CountDownLatch(4);
		for (int a = 0; a < 4; a++) {
			final int t = a;
			es.execute(new Runnable() {
				public void run() {
					for (int i = 0; i < size; i++) {
						instances[t].getMap("default").get(i);
					}
					latch.countDown();
				}
			});
		}
		latch.await();
		es.shutdown();
		assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
		waitForGC(25 + usedMemoryInit, 100);
	}

	@Test
	public void testMaxIdleAndMemoryLeak() throws Exception {
		Runtime.getRuntime().gc();
		long usedMemoryInit = getUsedMemoryAsMB();
		Config config = new XmlConfigBuilder().build();
		MapConfig mapConfig = config.getMapConfig("default");
		mapConfig.setMaxIdleSeconds(15);
		final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(config);
		final HazelcastInstance[] instances = new HazelcastInstance[4];
		instances[0] = h1;
		instances[1] = h2;
		instances[2] = h3;
		instances[3] = h4;
		IMap map1 = h1.getMap("default");
		final int size = 10000;
		for (int i = 0; i < size; i++) {
			map1.put(i, new byte[10000]);
		}
		long usedMemoryStart = getUsedMemoryAsMB();
		assertTrue("UsedMemoryStart: " + usedMemoryStart, usedMemoryStart > 200);
		final ExecutorService es = Executors.newFixedThreadPool(4);
		final CountDownLatch latch = new CountDownLatch(4);
		for (int a = 0; a < 4; a++) {
			final int t = a;
			es.execute(new Runnable() {
				public void run() {
					for (int i = 0; i < size; i++) {
						instances[t].getMap("default").get(i);
					}
					latch.countDown();
				}
			});
		}
		latch.await();
		es.shutdown();
		assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
		waitForGC(25 + usedMemoryInit, 100);
	}

	long getUsedMemoryAsMB() {
		long total = Runtime.getRuntime().totalMemory();
		long free = Runtime.getRuntime().freeMemory();
		return (total - free) / 1024 / 1024;
	}

}
