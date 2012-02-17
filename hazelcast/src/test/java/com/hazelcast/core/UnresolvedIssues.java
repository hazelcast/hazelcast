/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * UnresolvedIssues is a set of unit test for known issues
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class UnresolvedIssues extends TestUtil {

    @Before
    @After
    public void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
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
        // this passes now!!
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
//        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IMap map = h1.getMap("default");
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
}
