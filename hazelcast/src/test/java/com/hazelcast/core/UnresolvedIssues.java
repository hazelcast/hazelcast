/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import java.io.Serializable;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.ReplicatedMapFactory;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * UnresolvedIssues is a set of unit test for known issues 
 */
public class UnresolvedIssues {

    @Before
    @After
    public void init() throws Exception {
        Hazelcast.shutdownAll();
    }
    
    @Ignore
    @Test
    public void issue370() throws Exception {
        // failed
        
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Queue<String> q1 = h1.getQueue("q");
        Queue<String> q2 = h2.getQueue("q");
        for (int i = 0; i < 5; i++) {
            q1.offer("item" + i);
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());
        assertEquals("item0", q2.poll());
        assertEquals("item1", q2.poll());
        assertEquals("item2", q2.poll());
        Thread.sleep(10000);
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        h1.shutdown();
        Thread.sleep(5000);
        assertEquals(2, q2.size());

        h1 = Hazelcast.newHazelcastInstance(null);
        q1 = h1.getQueue("q");
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        Thread.sleep(5000);
        h2.shutdown();
        Thread.sleep(5000);
        assertEquals(2, q1.size());
    }
    
    @Ignore
    @Test
    public void issue371NearCachePutGetRemove() throws Exception {
     // looks like passed ok
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(new XmlConfigBuilder(ClassLoader.getSystemResourceAsStream("hazelcast-issue371.xml")).build());
        IMap<Object, Object> cache = hz.getMap("ipp-2nd-level-cache-near");

        assertNotNull(cache);

        Object value = cache.get("my-key");
        assertNull(value);

        value = cache.put("my-key", "my-value");
        assertNull(value);

        value = cache.get("my-key");
        assertEquals("my-value", value);

        value = cache.remove("my-key");
        assertEquals("my-value", value);

        value = cache.get("my-key");
        assertNull(value);
    }

    @Ignore
    @Test
    public void issue371NearCachePutContainsNonexistentKey() throws Exception {
        // looks like passed ok
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(new XmlConfigBuilder(ClassLoader.getSystemResourceAsStream("hazelcast-issue371.xml")).build());
        IMap<Object, Object> cache = hz.getMap("ipp-2nd-level-cache-near");

        assertNotNull(cache);

        Object value = cache.get("my-key");
        assertNull(value);

        boolean foundKey = cache.containsKey("my-key");
        assertFalse(foundKey);

        value = cache.remove("my-key");
        assertNull(value);

        value = cache.get("my-key");
        assertNull(value);
    }

    @Ignore
    @Test
    public void issue371NearCachePutContainsExistentKey() throws Exception {
        // hangs on, issue: 
        // java.lang.IllegalStateException: Removed CacheEntry cannot be null
        // at com.hazelcast.impl.MapNearCache.invalidate(MapNearCache.java:181)
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(new XmlConfigBuilder(ClassLoader.getSystemResourceAsStream("hazelcast-issue371.xml")).build());
        IMap<Object, Object> cache = hz.getMap("ipp-2nd-level-cache-near");

        assertNotNull(cache);

        Object value = cache.get("my-key");
        assertNull(value);

        value = cache.put("my-key", "my-value");
        assertNull(value);

        boolean foundKey = cache.containsKey("my-key");
        assertTrue(foundKey);

        value = cache.remove("my-key");
        assertEquals("my-value", value);

        value = cache.get("my-key");
        assertNull(value);
    }

    
    @Ignore
    @Test
    public void issue386() {
        // failed with OutOfMemoryError
        IMap<Object, Object> map = Hazelcast.getMap("default");
        int maxLoopCount = 10000000;
        for (int count = 0; count < maxLoopCount; count++) {
            if (count % 10000 == 0) {
                System.out.println("lock " + count);
            }
            String o = Integer.toString(count);
            map.lock(o);
            try {
            } finally {
                map.unlock(o);
            }
        }
    }
    
    @Ignore
    @Test
    public void issue387() {
        // fail with java.lang.ClassCastException: 
        // java.util.concurrent.ConcurrentHashMap$WriteThroughEntry cannot be cast to com.hazelcast.core.MapEntry
        IMap cache = ReplicatedMapFactory.getMap("exampleMap");
        ExampleObject object1 = new ExampleObject("1", "Value");

        cache.put(object1.getId(), object1);

        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("id").equal("1");
        Collection<Object> queryResultCollection = cache.values(predicate);
    }
    
    static class ExampleObject implements Serializable {
        private static final long serialVersionUID = -5966060029235919352L;
        private String id;
        private String value;

        public ExampleObject(String id, String value) {
            this.id = id;
            this.value = value;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }


    @Ignore
    @Test
    public void issue391() throws Exception{
        // passed
        final Collection<String> results = new CopyOnWriteArrayList<String>();
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(null);
        new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < 5; i++) {
                        results.add((String)hz1.getQueue("q").take());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 5; i++) {
                    hz2.getQueue("q").offer(Integer.toString(i));
                }
            }
        }).start();

        
        Thread.sleep(5000);
        assertTrue(hz1.getQueue("q").isEmpty());
        hz1.getLifecycleService().shutdown();
        assertTrue(hz2.getQueue("q").isEmpty());
        
        assertArrayEquals(new Object[]{"0", "1", "2", "3", "4"}, results.toArray());
    }
    
    @Ignore
    @Test
    public void issue392() {
        // failed
        ITopic<Object> topic = Hazelcast.getTopic("default");
        assertEquals(1, Hazelcast.getInstances().size());
        for(Instance instance : Hazelcast.getInstances()) {
            assertEquals(InstanceType.TOPIC, instance.getInstanceType());
        }
        topic.destroy();
        assertEquals(0, Hazelcast.getInstances().size());
    }
}
