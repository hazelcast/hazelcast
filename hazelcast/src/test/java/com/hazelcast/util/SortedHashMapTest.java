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

package com.hazelcast.util;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SortedHashMapTest {

    private SortedHashMap<String, String> map = new SortedHashMap<String, String>();

    @After
    public void clear() {
        map.clear();
    }

    @Test
    public void testPutAndGet() {
        map.put("hello", "world");
        assertEquals("world", map.get("hello"));
    }

    @Test
    public void testTouch() {
        map.put("hello", "world");
        long updateTime = Clock.currentTimeMillis();
        SortedHashMap.touch(map, "hello", SortedHashMap.OrderingType.LFU);
        long lastAccess = map.getEntry("hello").lastAccess;
        assertTrue(Math.abs(updateTime - lastAccess) < 100);
    }

    @Test
    public void testMoveToTop() {
        for (int i = 0; i < 10; ++i) {
            map.put("hello" + i, "world");
        }
        SortedHashMap.moveToTop(map, "hello9");
        Set<String> keys = map.keySet();
        assertEquals("hello9", keys.iterator().next());
    }

    @Test
    public void testGet() {
        map.put("hello", "world");
        assertEquals("world", map.get("hello"));
    }

    @Test
    public void testRemove() {
        map.put("hello", "world");
        map.remove("hello");
        assertEquals(0, map.size());
    }

    @Test
    public void testContainsValue() {
        map.put("hello", "world");
        assertTrue(map.containsValue("world"));
    }

    @Test
    public void testKeySet() {
        for (int i = 0; i < 10; ++i) {
            map.put("hello" + i, "world");
        }
        Set<String> keys = map.keySet();
        int i = 0;
        for (String key : keys) {
            assertEquals("hello" + i++, key);
        }
    }

    @Test
    public void testValues() {
        for (int i = 0; i < 10; ++i) {
            map.put("hello" + i, "world" + i);
        }
        Collection<String> values = map.values();
        int i = 0;
        for (String value : values) {
            assertEquals("world" + i++, value);
        }
    }

    @Test
    public void testEntrySet() {
        for (int i = 0; i < 10; ++i) {
            map.put("hello" + i, "world" + i);
        }
        Set<Map.Entry<String, String>> entries = map.entrySet();
        int i = 0;
        for (Map.Entry<String, String> entry : entries) {
            assertEquals("hello" + i, entry.getKey());
            assertEquals("world" + i++, entry.getValue());
        }
    }
}
