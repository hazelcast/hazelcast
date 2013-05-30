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
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class ListenerTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    private HazelcastInstance h1;
    private HazelcastInstance h2;
    private IMap<String, String> map1;
    private IMap<String, String> map2;

    private AtomicInteger globalCount = new AtomicInteger();
    private AtomicInteger localCount = new AtomicInteger();
    private AtomicInteger valueCount = new AtomicInteger();

    @Before
    public void before() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        h1 = nodeFactory.newHazelcastInstance(cfg);
        h2 = nodeFactory.newHazelcastInstance(cfg);
        createMaps();
    }

    @After
    public void after() {
        destroyMaps();
    }

    private void createMaps() {
        globalCount.set(0);
        localCount.set(0);
        valueCount.set(0);
        map1 = h1.getMap(name);
        map2 = h2.getMap(name);
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
    public void globalListenerRemoveTest() throws InterruptedException {
        String id1 = map1.addEntryListener(createEntryListener(false), false);
        String id2 = map1.addEntryListener(createEntryListener(false), true);
        String id3 = map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        map1.removeEntryListener(id1);
        map1.removeEntryListener(id2);
        map1.removeEntryListener(id3);
        putDummyData(k);
        checkCountWithExpected(0, 0, 0);
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
            }
        };
    }


}
