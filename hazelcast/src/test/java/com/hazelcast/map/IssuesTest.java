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
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
        final EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.SECONDS);
        final EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.SECONDS);
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
        final EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.SECONDS);
        final EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.SECONDS);
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
        final EntryEvent<Integer, Integer> event1 = events.poll(10, TimeUnit.SECONDS);
        final EntryEvent<Integer, Integer> event2 = events.poll(10, TimeUnit.SECONDS);
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

    @Test
    public void testIssue1067GlobalSerializer() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        final Config config = new Config();
        config.getSerializationConfig().setGlobalSerializerConfig(new GlobalSerializerConfig()
                .setImplementation(new StreamSerializer() {
                    public void write(ObjectDataOutput out, Object object) throws IOException {
                    }
                    public Object read(ObjectDataInput in) throws IOException {
                        return new DummyValue();
                    }
                    public int getTypeId() {
                        return 123;
                    }
                    public void destroy() {
                    }
                }));

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = hz.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new DummyValue());
        }
        assertEquals(10, map.size());

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map2 = hz2.getMap("test");
        assertEquals(10, map2.size());
        assertEquals(10, map.size());

        for (int i = 0; i < 10; i++) {
            Object o = map2.get(i);
            assertNotNull(o);
            assertTrue(o instanceof DummyValue);
        }
    }

    private static class DummyValue {
    }

    @Test
    public void testMapInterceptorInstanceAware() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        IMap<Object,Object> map = hz1.getMap("test");

        InstanceAwareMapInterceptorImpl interceptor = new InstanceAwareMapInterceptorImpl();
        map.addInterceptor(interceptor);
        assertNotNull(interceptor.hazelcastInstance);
        assertEquals(hz1, interceptor.hazelcastInstance);

        for (int i=0; i<100; i++){
            map.put(i, i);
        }

        for (int i=0; i<100; i++){
            assertEquals("notNull", map.get(i));
        }
    }

    static class InstanceAwareMapInterceptorImpl implements MapInterceptor, HazelcastInstanceAware {

        transient HazelcastInstance hazelcastInstance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {

        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            if (hazelcastInstance != null){
                return "notNull";
            }
            return ">null";
        }

        @Override
        public void afterPut(Object value) {

        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {

        }
    }

}
