/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongSetHsaTest {

    private final Random random = new Random();
    private MemoryManager memMgr;
    private LongSet set;

    @Before
    public void setUp() {
        memMgr = new HsaHeapMemoryManager();
        set = new LongSetHsa(0L, memMgr);
    }

    @After
    public void tearDown() {
        set.dispose();
        memMgr.dispose();
    }

    @Test
    public void testAdd() {
        long key = random.nextLong();
        assertTrue(set.add(key));
        assertFalse(set.add(key));
    }

    @Test
    public void testRemove() {
        long key = random.nextLong();
        assertFalse(set.remove(key));
        set.add(key);
        assertTrue(set.remove(key));
    }

    @Test
    public void testAddRemoveMany() {
        int upperBound = 10000;
        for (int i = 1; i < upperBound; i++) {
            assertTrue(set.add(i));
        }
        for (int i = 1; i < upperBound; i++) {
            assertTrue(set.contains(i));
        }
        for (int i = upperBound; i < 2 * upperBound; i++) {
            assertFalse(set.contains(i));
        }
        for (int i = 1; i < upperBound; i += 3) {
            assertTrue(set.remove(i));
        }
        for (int i = 1; i < upperBound; i++) {
            if ((i - 1) % 3 == 0) {
                assertFalse(set.contains(i));
            } else {
                assertTrue(set.contains(i));
            }
        }
    }

    @Test
    public void testContains() {
        long key = random.nextLong();
        assertFalse(set.contains(key));
        set.add(key);
        assertTrue(set.contains(key));
    }

    @Test
    public void testCursor() {
        assertFalse(set.cursor().advance());

        Set<Long> expected = new HashSet<Long>();
        for (int i = 1; i <= 1000; i++) {
            set.add(i);
            expected.add((long) i);
        }

        LongCursor cursor = set.cursor();
        while (cursor.advance()) {
            long key = cursor.value();
            assertTrue("Key: " + key, expected.remove(key));
        }
    }

    @Test
    public void testClear() {
        for (int i = 1; i <= 1000; i++) {
            set.add(i);
        }

        set.clear();
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(0, set.size());

        int expected = 1000;
        for (int i = 1; i <= expected; i++) {
            set.add(i);
        }

        assertEquals(expected, set.size());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(set.isEmpty());

        set.add(random.nextLong());

        assertFalse(set.isEmpty());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testAdd_after_destroy() {
        set.dispose();
        set.add(1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testRemove_after_destroy() {
        set.dispose();
        set.remove(1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testContains_after_destroy() {
        set.dispose();
        set.contains(1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_after_destroy() {
        set.dispose();
        set.cursor();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_after_destroy2() {
        set.add(1);
        LongCursor cursor = set.cursor();
        set.dispose();
        cursor.advance();
    }
}
