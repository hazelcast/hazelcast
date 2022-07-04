/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.locksupport.LockStoreContainer;
import com.hazelcast.internal.locksupport.LockStoreImpl;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.locksupport.LockSupportServiceImpl;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLockTest extends HazelcastTestSupport {

    @Test
    public void testIsLocked_afterDestroy() {
        final IMap<String, String> map = getMap();
        final String key = randomString();
        map.lock(key);
        map.destroy();
        assertFalse(map.isLocked(key));
    }

    @Test
    public void testIsLocked_afterDestroy_whenMapContainsKey() {
        final IMap<String, String> map = getMap();
        final String key = randomString();
        map.put(key, "value");
        map.lock(key);
        map.destroy();
        assertFalse(map.isLocked(key));
    }

    @Test
    public void testBackupDies() throws TransactionException {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<Integer, Integer> map1 = h1.getMap("testBackupDies");
        final int size = 50;
        final CountDownLatch latch = new CountDownLatch(1);

        Runnable runnable = new Runnable() {
            public void run() {
                for (int i = 0; i < size; i++) {
                    map1.lock(i);
                    sleepMillis(100);
                }
                for (int i = 0; i < size; i++) {
                    assertTrue(map1.isLocked(i));
                }
                for (int i = 0; i < size; i++) {
                    map1.unlock(i);
                }
                for (int i = 0; i < size; i++) {
                    assertFalse(map1.isLocked(i));
                }
                latch.countDown();
            }
        };
        new Thread(runnable).start();
        try {
            sleepSeconds(1);
            h2.shutdown();
            assertTrue(latch.await(30, TimeUnit.SECONDS));
            for (int i = 0; i < size; i++) {
                assertFalse(map1.isLocked(i));
            }
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testLockEviction() throws Exception {
        final String name = randomString();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = getConfig();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(instance2, instance1);

        final IMap<Integer, Integer> map = instance1.getMap(name);
        map.put(1, 1);
        map.lock(1, 1, TimeUnit.SECONDS);
        assertTrue(map.isLocked(1));
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                map.lock(1);
                latch.countDown();
            }
        });
        t.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockTTL_whenZeroTimeout() throws Exception {
        final IMap<String, String> mm = getMap();
        final String key = "Key";
        mm.lock(key, 0, TimeUnit.SECONDS);
    }

    @Test
    public void testLockEviction2() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = getConfig();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(instance2, instance1);

        final String name = randomString();
        final IMap<Integer, Integer> map = instance1.getMap(name);
        Random rand = new Random();
        for (int i = 0; i < 5; i++) {
            map.lock(i, rand.nextInt(5) + 1, TimeUnit.SECONDS);
        }
        final CountDownLatch latch = new CountDownLatch(5);
        Thread t = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 5; i++) {
                    map.lock(i);
                    latch.countDown();
                }
            }
        });
        t.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLockMigration() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = getConfig();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final String name = randomString();
        final IMap<Object, Object> map = instance1.getMap(name);
        for (int i = 0; i < 1000; i++) {
            map.lock(i);
        }

        nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        Thread.sleep(3000);
        final CountDownLatch latch = new CountDownLatch(1000);
        Thread t = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                if (map.isLocked(i)) {
                    latch.countDown();
                }
            }
        });
        t.start();
        assertTrue(latch.await(15, TimeUnit.SECONDS));
    }

    @Category(NightlyTest.class)
    @Test
    public void testLockEvictionWithMigration() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = getConfig();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final String name = randomString();
        final IMap<Integer, Object> map = instance1.getMap(name);
        for (int i = 0; i < 1000; i++) {
            map.lock(i, 20, TimeUnit.SECONDS);
        }
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(config);
        for (int i = 0; i < 1000; i++) {
            assertTrue(map.isLocked(i));
        }
        final CountDownLatch latch = new CountDownLatch(1000);
        Thread t = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    map.lock(i);
                    latch.countDown();
                }
            }
        });
        t.start();
        assertOpenEventually(latch);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testLockOwnership() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = getConfig();
        String name = randomString();

        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance node2 = nodeFactory.newHazelcastInstance(config);

        final IMap<Integer, Object> map1 = node1.getMap(name);
        final IMap<Integer, Object> map2 = node2.getMap(name);

        map1.lock(1);
        map2.unlock(1);
    }

    @Test
    public void testAbsentKeyIsLocked() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config config = getConfig();
        final String name = randomString();
        final String key = "KEY";
        final String val = "VAL_2";

        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance node2 = nodeFactory.newHazelcastInstance(config);

        final IMap<String, String> map1 = node1.getMap(name);
        final IMap<String, String> map2 = node2.getMap(name);

        map1.lock(key);
        boolean putResult = map2.tryPut(key, val, 2, TimeUnit.SECONDS);

        assertFalse("the result of try put should be false as the absent key is locked", putResult);
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals("the key should be absent ", null, map1.get(key));
                assertEquals("the key should be absent ", null, map2.get(key));
            }
        });
    }

    @Test
    public void testLockTTLKey() {

        final IMap<String, String> map = getMap();
        final String key = "key";
        final String val = "val";

        final int TTL_SEC = 1;
        map.lock(key);
        map.put(key, val, TTL_SEC, TimeUnit.SECONDS);

        sleepSeconds(TTL_SEC * 2);

        assertEquals("TTL of KEY has expired, KEY is locked, we expect VAL", val, map.get(key));
        map.unlock(key);
        assertEquals("TTL of KEY has expired, KEY is unlocked, we expect null", null, map.get(key));
    }

    @Test
    public void testClear_withLockedKey() {
        final IMap<String, String> map = getMap();
        final String KEY = "key";
        final String VAL = "val";

        map.put(KEY, VAL);
        map.lock(KEY);
        map.clear();

        assertEquals("a locked key should not be removed by map clear", false, map.isEmpty());
        assertEquals("a key present in a map, should be locked after map clear", true, map.isLocked(KEY));
    }

    /**
     * Do not use ungraceful node.getLifecycleService().terminate(), because that leads
     * backup inconsistencies between nodes and eventually this test will fail.
     * Instead use graceful node.getLifecycleService().shutdown().
     */
    @Test
    public void testClear_withLockedKey_whenNodeShutdown() {
        Config config = getConfig();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance node2 = nodeFactory.newHazelcastInstance(config);

        final String mapName = randomString();
        final IMap<Object, Object> map = node2.getMap(mapName);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        String key = generateKeyOwnedBy(node2);
        map.put(key, "value");
        map.lock(key);

        final CountDownLatch cleared = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.clear();
                cleared.countDown();
            }
        }.start();

        assertOpenEventually(cleared);

        node1.getLifecycleService().shutdown();

        assertTrue("a key present in a map, should be locked after map clear", map.isLocked(key));
        assertEquals("unlocked keys not removed", 1, map.size());
    }

    @Test
    public void testTryLockLeaseTime_whenLockFree() throws InterruptedException {
        IMap<String, String> map = getMap();
        String key = randomString();
        boolean isLocked = map.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }

    @Test
    public void testTryLockLeaseTime_whenLockAcquiredByOther() throws InterruptedException {
        final IMap<String, String> map = getMap();
        final String key = randomString();
        Thread thread = new Thread() {
            public void run() {
                map.lock(key);
            }
        };
        thread.start();
        thread.join();

        boolean isLocked = map.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertFalse(isLocked);
    }

    @Test
    public void testTryLockLeaseTime_lockIsReleasedEventually() throws InterruptedException {
        final IMap<String, String> map = getMap();
        final String key = randomString();
        map.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(map.isLocked(key));
            }
        }, 30);
    }

    /**
     * See issue #4888
     */
    @Test
    public void lockStoreShouldBeRemoved_whenMapIsDestroyed() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = instance.getMap(randomName());
        for (int i = 0; i < 1000; i++) {
            map.lock(i);
        }
        map.destroy();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        LockSupportServiceImpl lockService = nodeEngine.getService(LockSupportService.SERVICE_NAME);
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            LockStoreContainer lockContainer = lockService.getLockContainer(i);
            Collection<LockStoreImpl> lockStores = lockContainer.getLockStores()
                    .stream()
                    .filter(s -> !s.getNamespace().getObjectName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX))
                    .collect(Collectors.toList());
            assertEquals("LockStores should be empty", 0, lockStores.size());
        }
    }

    protected <K, V> IMap<K, V> getMap() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        return instance.getMap(randomString());
    }
}
