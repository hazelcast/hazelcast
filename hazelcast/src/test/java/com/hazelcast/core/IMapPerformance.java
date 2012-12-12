/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class IMapPerformance extends PerformanceTest {

    private IMap map = Hazelcast.getMap("IMapPerformance");

    @After
    public void clear() {
        t.stop();
        t.printResult();
        map.clear();
    }

    @Test
    public void testMapPutWithSameKey() {
        t = new PerformanceTimer("testMapPutWithSameKey", ops);
        for (int i = 0; i < ops; ++i) {
            map.put("Hello", "World");
        }
    }

    @Test
    public void testMapPutWithDifferentKey() {
        t = new PerformanceTimer("testMapPutWithDifferentKey", ops);
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
    }

    @Test
    public void testMapGet() {
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
        t = new PerformanceTimer("testMapGet", ops);
        for (int i = 0; i < ops; ++i) {
            map.get(i);
        }
    }

    @Test
    public void testMapGetMapEntry() {
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
        t = new PerformanceTimer("testMapGetMapEntry", ops);
        for (int i = 0; i < ops; ++i) {
            map.getMapEntry(i);
        }
    }

    @Test
    public void testMapRemove() {
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
        t = new PerformanceTimer("testMapRemove", ops / 10);
        for (int i = 0; i < ops / 10; ++i) {
            int index = (int) ((int) ops * Math.random());
            map.remove(index);
        }
    }

    @Test
    public void testMapValues() {
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
        t = new PerformanceTimer("testMapValues", ops);
        for (Object value : map.values()) {
            Object a = value;
        }
    }

    @Test
    public void testMapReplace() {
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
        t = new PerformanceTimer("testMapReplace", ops);
        for (int i = 0; i < ops; ++i) {
            map.replace(i, "FooBar");
        }
    }

    @Test
    public void testMapContainsKey() {
        String test = "testMapContainsKey";
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World");
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            map.containsKey(i);
        }
    }

    @Test
    public void testMapContainsValue() {
        String test = "testMapContainsValue";
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World" + i);
        }
        t = new PerformanceTimer(test, ops);
        for (int i = 0; i < ops; ++i) {
            map.containsValue("Hello World" + i);
        }
    }

    @Test
    public void testMapKeySet() {
        String test = "testMapKeySet";
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World" + i);
        }
        t = new PerformanceTimer(test, ops);
        map.keySet();
    }

    @Test
    public void testMapEntrySet() {
        String test = "testMapEntrySet";
        for (int i = 0; i < ops; ++i) {
            map.put(i, "Hello World" + i);
        }
        t = new PerformanceTimer(test, ops);
        map.entrySet();
    }
}
