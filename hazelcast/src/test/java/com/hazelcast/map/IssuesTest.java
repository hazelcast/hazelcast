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
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class IssuesTest extends HazelcastTestSupport {

    @Test
    public void testIssue321_1() throws Exception {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        final IMap<Integer, Integer> imap = factory.newHazelcastInstance(null).getMap("testIssue321_1");
        final BlockingQueue<EntryEvent<Integer, Integer>> events1 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final BlockingQueue<EntryEvent<Integer, Integer>> events2 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        imap.put(1, 1);
        final EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.MILLISECONDS);
        final EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_2() throws Exception {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        final IMap<Integer, Integer> imap = factory.newHazelcastInstance(null).getMap("testIssue321_2");
        final BlockingQueue<EntryEvent<Integer, Integer>> events1 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final BlockingQueue<EntryEvent<Integer, Integer>> events2 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        Thread.sleep(50L);
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        imap.put(1, 1);
        final EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.MILLISECONDS);
        final EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_3() throws Exception {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        final IMap<Integer, Integer> imap = factory.newHazelcastInstance(null).getMap("testIssue321_3");
        final BlockingQueue<EntryEvent<Integer, Integer>> events = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final EntryAdapter<Integer, Integer> listener = new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events.add(event);
            }
        };
        imap.addEntryListener(listener, true);
        Thread.sleep(50L);
        imap.addEntryListener(listener, false);
        imap.put(1, 1);
        final EntryEvent<Integer, Integer> event1 = events.poll(10, TimeUnit.MILLISECONDS);
        final EntryEvent<Integer, Integer> event2 = events.poll(10, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue304() {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        IMap<String, String> map = factory.newHazelcastInstance(null).getMap("testIssue304");
        map.lock("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
        map.put("1", "value");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.unlock("1");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
    }

    /*
       github issue 174
    */
    @Test
    public void testIssue174NearCacheContainsKeySingleNode() {
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        Config config = new Config();
        config.getGroupConfig().setName("testIssue174NearCacheContainsKeySingleNode");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue174NearCacheContainsKeySingleNode");
        map.put("key", "value");
        assertTrue(map.containsKey("key"));
        h.getLifecycleService().shutdown();
    }

}
