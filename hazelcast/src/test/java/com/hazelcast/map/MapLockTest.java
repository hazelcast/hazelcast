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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapLockTest extends HazelcastTestSupport {

    @Test
    public void testIsLocked_afterDestroy (){
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Object, Object> map = instance.getMap(randomString());
        final String key = randomString();
        map.lock(key);
        map.destroy();
        Assert.assertFalse(map.isLocked(key));
    }

    @Test
    public void testBackupDies() throws TransactionException {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map1 = h1.getMap("testBackupDies");
        final int size = 50;
        final CountDownLatch latch = new CountDownLatch(size + 1);

        Runnable runnable = new Runnable() {
            public void run() {
                for (int i = 0; i < size; i++) {
                    map1.lock(i);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                    latch.countDown();
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
            Thread.sleep(1000);
            h2.shutdown();
            latch.await();
            for (int i = 0; i < size; i++) {
                assertFalse(map1.isLocked(i));
            }
        } catch (InterruptedException e) {
        }
    }

    @Test(timeout = 20000)
    public void testLockEviction() throws Exception {
        final String mapName = "testLockEviction";
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(1);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(instance2, instance1);

        final IMap map = instance1.getMap(mapName);
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
        final Config config = new Config();
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        final IMap mm = instance.getMap(randomString());
        final Object key = "Key";
        mm.lock(key, 0, TimeUnit.SECONDS);
    }

    @Test(timeout = 100000)
    public void testLockEviction2() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(instance2, instance1);

        final String name = "testLockEviction2";
        final IMap map = instance1.getMap(name);
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

    @Test(timeout = 100000)
    public void testLockMigration() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = new Config();
        final AtomicInteger integer = new AtomicInteger(0);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final String name = "testLockMigration";
        final IMap map = instance1.getMap(name);
        for (int i = 0; i < 1000; i++) {
            map.lock(i);
        }

        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(config);
        Thread.sleep(3000);
        final CountDownLatch latch = new CountDownLatch(1000);
        Thread t = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    if (map.isLocked(i)) {
                        latch.countDown();
                    }
                }
            }
        });
        t.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 100000)
    public void testLockEvictionWithMigration() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        final Config config = new Config();
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final String name = "testLockEvictionWithMigration";
        final IMap map = instance1.getMap(name);
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

    @Test(timeout = 1000*15, expected = IllegalMonitorStateException.class)
    public void testLockOwnership() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final Config config = new Config();

        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance node2 = nodeFactory.newHazelcastInstance(config);

        final IMap map1 = node1.getMap("map");
        final IMap map2 = node2.getMap("map");

        map1.lock(1);
        map2.unlock(1);
    }

    @Test(timeout = 1000*30)
    public void testAbsentKeyIsLocked() throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        final String MAP_A = "MAP_A";
        final String KEY = "KEY";
        final String VAL_2 = "VAL_2";

        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance node2 = nodeFactory.newHazelcastInstance();

        final IMap map1 = node1.getMap(MAP_A);
        final IMap map2 = node2.getMap(MAP_A);

        map1.lock(KEY);
        boolean putResult = map2.tryPut(KEY, VAL_2, 2, TimeUnit.SECONDS);

        Assert.assertFalse("the result of try put should be false as the absent key is locked", putResult);
        assertTrueEventually(new AssertTask() {
            public void run() {
                Assert.assertEquals("the key should be absent ", null, map1.get(KEY));
                Assert.assertEquals("the key should be absent ", null, map2.get(KEY));
            }
        });
    }

    @Test
    public void testLockTTLKey() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance();

        final IMap map = node1.getMap("map");
        final String KEY = "key";
        final String VAL = "val";

        final int TTL_SEC = 1;
        map.put(KEY, VAL, TTL_SEC, TimeUnit.SECONDS);
        map.lock(KEY);

        sleepSeconds(TTL_SEC * 2);

        assertEquals("TTL of KEY has expired, KEY is locked, we expect VAL", VAL, map.get(KEY));
        map.unlock(KEY);
        assertEquals("TTL of KEY has expired, KEY is unlocked, we expect null", null, map.get(KEY));
    }


    @Test
    public void testClear_withLockedKey() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance();

        final IMap map = node1.getMap("map");
        final String KEY = "key";
        final String VAL = "val";

        map.put(KEY, VAL);
        map.lock(KEY);
        map.clear();

        assertEquals("a locked key should not be removed by map clear", false, map.isEmpty());
        assertEquals("a key present in a map, should be locked after map clear", true, map.isLocked(KEY));
    }

    @Test
    public void testClear_withLockedKey_whenNodeTerminated() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance node2 = nodeFactory.newHazelcastInstance();

        final IMap map = node1.getMap(randomString());

        for(int i=0; i<1000; i++){
            map.put(i, i);
        }

        Object key = generateKeyOwnedBy(node2);
        map.put(key, "value");
        map.lock(key);

        final CountDownLatch cleared = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.clear();
                cleared.countDown();
            }
        }.start();

        assertOpenEventually(cleared);

        node2.getLifecycleService().terminate();

        assertEquals("unlocked keys not removed", 1, map.size());
        assertEquals("a key present in a map, should be locked after map clear", true, map.isLocked(key));
    }
}
