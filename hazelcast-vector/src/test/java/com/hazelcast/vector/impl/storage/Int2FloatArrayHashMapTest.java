/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Int2FloatArrayHashMapTest {

    private Int2FloatArrayHashMap map;
    float[] value = new float[] {1};

    @BeforeEach
    void setup() {
        map = new Int2FloatArrayHashMap(1);
    }

    @Test
    void testContainsKey() {
        map.put(1, value);
        assertTrue(map.containsKey(1));
        assertFalse(map.containsKey(2));
    }

    @Test
    void testGet() {
        map.put(1, value);
        assertNotNull(map.get(1));
        assertSame(value, map.get(1));
        assertNull(map.get(2));
    }

    @Test
    void testComputeIfAbsent() {
        map.computeIfAbsent(1, i -> value);
        assertSame(value, map.get(1));
    }

    @Test
    void testRemove() {
        map.put(1, value);
        float[] removedValue = map.remove(1);
        assertSame(value, removedValue);
        assertNull(map.get(1));
    }
}
