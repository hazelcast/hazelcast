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
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.core.*;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.*;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ListenerTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    private final AtomicInteger globalCount = new AtomicInteger();
    private final AtomicInteger localCount = new AtomicInteger();
    private final AtomicInteger valueCount = new AtomicInteger();

    @Before
    public void before() {
        globalCount.set(0);
        localCount.set(0);
        valueCount.set(0);
    }

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        Config config = new Config();
        final String name = "default";
        final CountDownLatch latch = new CountDownLatch(1);
        config.getMapConfig(name).addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        }));
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getMap(name).put(1, 1);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void globalListenerTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        map1.addEntryListener(createEntryListener(false), false);
        map1.addEntryListener(createEntryListener(false), true);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        putDummyData(map1, k);
        checkCountWithExpected(k * 3, 0, k * 2);
    }

    @Test
    public void globalListenerRemoveTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        String id1 = map1.addEntryListener(createEntryListener(false), false);
        String id2 = map1.addEntryListener(createEntryListener(false), true);
        String id3 = map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        map1.removeEntryListener(id1);
        map1.removeEntryListener(id2);
        map1.removeEntryListener(id3);
        putDummyData(map2, k);
        checkCountWithExpected(0, 0, 0);
    }

    @Test
    public void localListenerTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        int k = 4;
        putDummyData(map1, k);
        checkCountWithExpected(0, k, k);
    }

    @Test
    /**
     * Test for issue 584 and 756
     */
    public void globalAndLocalListenerTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        map1.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 1;
        putDummyData(map2, k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    @Test
    /**
     * Test for issue 584 and 756
     */
    public void globalAndLocalListenerTest2() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        // changed listener order
        map1.addEntryListener(createEntryListener(false), false);
        map1.addLocalEntryListener(createEntryListener(true));
        map2.addEntryListener(createEntryListener(false), true);
        map2.addLocalEntryListener(createEntryListener(true));
        map2.addEntryListener(createEntryListener(false), false);
        int k = 3;
        putDummyData(map1, k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    private static void putDummyData(IMap map, int k) {
        for (int i = 0; i < k; i++) {
            map.put("foo" + i, "bar");
        }
    }

    private void checkCountWithExpected(int expectedGlobal, int expectedLocal, int expectedValue) throws InterruptedException {
        // wait for entry listener execution
        Thread.sleep(1000 * 3);
        Assert.assertEquals(expectedLocal, localCount.get());
        Assert.assertEquals(expectedGlobal, globalCount.get());
        Assert.assertEquals(expectedValue, valueCount.get());
    }

    /**
     * test for issue 589
     */
    @Test
    public void setFiresAlwaysAddEvent() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Object, Object> map = h1.getMap("map");
        final CountDownLatch updateLatch = new CountDownLatch(1);
        final CountDownLatch addLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryListener<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addLatch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updateLatch.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
            }
        }, false);
        map.set(1,1);
        map.set(1,2);
        assertTrue(addLatch.await(5, TimeUnit.SECONDS));
        assertTrue(updateLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testLocalEntryListener_singleInstance_with_MatchingPredicate() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        Config config = new Config();
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        IMap<String, String> map = instance.getMap("map");

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key"+i, "value"+i);
        }
        checkCountWithExpected(count, 0, 0);
    }

    @Test
    public void testLocalEntryListener_singleInstance_with_NonMatchingPredicate() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        Config config = new Config();
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        IMap<String, String> map = instance.getMap("map");

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), nonMatchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key"+i, "value"+i);
        }
        checkCountWithExpected(0, 0, 0);
    }

    @Test
    public void testLocalEntryListener_multipleInstance_with_MatchingPredicate() throws Exception {
        int instanceCount = 3;
        HazelcastInstance instance = createHazelcastInstanceFactory(instanceCount).newInstances(new Config())[0];

        String mapName = "myMap";
        IMap<String, String> map = instance.getMap(mapName);

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key"+i, "value"+i);
        }
        final int eventPerPartitionMin = count / instanceCount - count / 10;
        final int eventPerPartitionMax = count / instanceCount + count / 10;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue( globalCount.get() > eventPerPartitionMin && globalCount.get() < eventPerPartitionMax);
            }
        });
    }

    @Test
    public void testLocalEntryListener_multipleInstance_with_MatchingPredicate_and_Key() throws Exception {
        int instanceCount = 1;
        HazelcastInstance instance = createHazelcastInstanceFactory(instanceCount).newInstances(new Config())[0];

        String mapName = "myMap";
        IMap<String, String> map = instance.getMap(mapName);

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), "key500", includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key"+i, "value"+i);
        }
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue( globalCount.get() == 1);
            }
        });
    }


    private Predicate<String, String> matchingPredicate() {
        return new Predicate<String, String>() {
            @Override
            public boolean apply(Map.Entry<String, String> mapEntry) {
                return true;
            }
        };
    }

    private Predicate<String, String> nonMatchingPredicate() {
        return new Predicate<String, String>() {
            @Override
            public boolean apply(Map.Entry<String, String> mapEntry) {
                return false;
            }
        };
    }

    private EntryListener<String, String> createEntryListener(final boolean isLocal) {
        return new EntryListener<String, String>() {
            private final boolean local = isLocal;

            public void entryUpdated(EntryEvent<String, String> event) {
            }

            public void entryRemoved(EntryEvent<String, String> event) {
            }

            public void entryEvicted(EntryEvent<String, String> event) {
            }

            public void entryAdded(EntryEvent<String, String> event) {
                if (local) {
                    localCount.incrementAndGet();
                } else {
                    globalCount.incrementAndGet();
                }
                if (event.getValue() != null) {
                    valueCount.incrementAndGet();
                }
            }
        };
    }


}
