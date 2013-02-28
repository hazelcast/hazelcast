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

import com.hazelcast.core.*;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapEntryListenerTest {

    final String n = "foo";

    HazelcastInstance h1;
    HazelcastInstance h2;
    IMap<String, String> map1;
    IMap<String, String> map2;

    AtomicInteger globalCount = new AtomicInteger();
    AtomicInteger localCount = new AtomicInteger();
    AtomicInteger valueCount = new AtomicInteger();

    @BeforeClass
    @AfterClass
    public static void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        h1 = Hazelcast.newHazelcastInstance(null);
        h2 = Hazelcast.newHazelcastInstance(null);
        createMaps();
    }

    @After
    public void after() {
        destroyMaps();
        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    private void createMaps() {
        globalCount.set(0);
        localCount.set(0);
        valueCount.set(0);
        map1 = h1.getMap(n);
        map2 = h2.getMap(n);
    }

    private void destroyMaps() {
        map1.destroy();
        map2.destroy();
    }

    @Test
    public void globalListenerTest() throws InterruptedException {
        map1.addEntryListener(createEntryListener(false), false);
        map1.addEntryListener(createEntryListener(false), true);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        putDummyData(k);
        checkCountWithExpected(k * 3, 0, k * 2);
    }

    @Test
    public void localListenerTest() throws InterruptedException {
        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        int k = 4;
        putDummyData(k);
        checkCountWithExpected(0, k, k);
    }

    @Test
    /**
     * Test for issue 584 and 756
     */
    public void globalAndLocalListenerTest() throws InterruptedException {
        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        map1.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 1;
        putDummyData(k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    @Test
    /**
     * Test for issue 584 and 756
     */
    public void globalAndLocalListenerTest2() throws InterruptedException {
        // changed listener order
        map1.addEntryListener(createEntryListener(false), false);
        map1.addLocalEntryListener(createEntryListener(true));
        map2.addEntryListener(createEntryListener(false), true);
        map2.addLocalEntryListener(createEntryListener(true));
        map2.addEntryListener(createEntryListener(false), false);
        int k = 3;
        putDummyData(k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    @Test
    /**
     * Test for Issue 663
     */
    public void createAfterDestroyListenerTest() throws Exception {
        createMaps();
        localListenerTest();
        destroyMaps();
        createMaps();
        localListenerTest();
        destroyMaps();
        createMaps();
        globalListenerTest();
        destroyMaps();
        createMaps();
        globalListenerTest();
        destroyMaps();
    }

    private void putDummyData(int k) {
        for (int i = 0; i < k; i++) {
            map1.put("foo" + i, "bar");
        }
    }

    private void checkCountWithExpected(int expectedGlobal, int expectedLocal, int expectedValue) throws InterruptedException {
        // wait for entry listener execution
        Thread.sleep(1000 * 3);
        Assert.assertEquals(expectedLocal, localCount.get());
        Assert.assertEquals(expectedGlobal, globalCount.get());
        Assert.assertEquals(expectedValue, valueCount.get());
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
                System.out.println(event);
            }
        };
    }
}
