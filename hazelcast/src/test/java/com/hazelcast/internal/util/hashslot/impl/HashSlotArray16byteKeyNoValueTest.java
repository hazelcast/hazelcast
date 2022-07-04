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

package com.hazelcast.internal.util.hashslot.impl;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.HeapMemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray16byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashSlotArray16byteKeyNoValueTest {

    private final Random random = new Random();
    private MemoryManager memMgr;
    private HashSlotArray16byteKey hsa;

    @Before
    public void setUp() throws Exception {
        memMgr = new HeapMemoryManager(32 << 20);
        hsa = new HashSlotArray16byteKeyNoValue(0L, memMgr);
        hsa.gotoNew();
    }

    @After
    public void tearDown() throws Exception {
        hsa.dispose();
        memMgr.dispose();
    }

    @Test
    public void testPut() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();
        SlotAssignmentResult slot = hsa.ensure(key1, key2);
        assertTrue(slot.isNew());
        final long valueAddress = slot.address();
        assertNotEquals(NULL_ADDRESS, valueAddress);

        slot = hsa.ensure(key1, key2);
        assertFalse(slot.isNew());
        assertEquals(valueAddress, slot.address());
    }

    @Test
    public void testGet() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();
        final SlotAssignmentResult slot = hsa.ensure(key1, key2);

        final long valueAddress2 = hsa.get(key1, key2);
        assertEquals(slot.address(), valueAddress2);
    }

    @Test
    public void testRemove() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();
        hsa.ensure(key1, key2);

        assertTrue(hsa.remove(key1, key2));
        assertFalse(hsa.remove(key1, key2));
    }

    @Test
    public void testSize() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();

        hsa.ensure(key1, key2);
        assertEquals(1, hsa.size());

        assertTrue(hsa.remove(key1, key2));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testClear() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();

        hsa.ensure(key1, key2);
        hsa.clear();

        assertEquals(NULL_ADDRESS, hsa.get(key1, key2));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testPutGetMany() {
        final long factor = 123456;
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            assertTrue(hsa.ensure(key1, key2).isNew());
        }

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            assertNotEquals(NULL_ADDRESS, hsa.get(key1, key2));
        }
    }

    @Test
    public void testPutRemoveGetMany() {
        final long factor = 123456;
        final int k = 5000;
        final int mod = 100;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            assertTrue(hsa.ensure(key1, key2).isNew());
        }

        for (int i = mod; i <= k; i += mod) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            assertTrue(hsa.remove(key1, key2));
        }

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = hsa.get(key1, key2);

            if (i % mod == 0) {
                assertEquals(NULL_ADDRESS, valueAddress);
            } else {
                assertNotEquals(NULL_ADDRESS, valueAddress);
            }
        }
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testPut_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.ensure(1, 1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGet_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.get(1, 1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testRemove_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.remove(1, 1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testClear_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.clear();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_key1_withoutAdvance() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.key1();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_key2_withoutAdvance() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.key2();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_valueAddress_withoutAdvance() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.valueAddress();
    }

    @Test
    public void testCursor_advance_whenEmpty() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance() {
        hsa.ensure(randomKey(), randomKey());

        HashSlotCursor16byteKey cursor = hsa.cursor();
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance_afterAdvanceReturnsFalse() {
        hsa.ensure(randomKey(), randomKey());

        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.advance();
        cursor.advance();

        assertThrows(AssertionError.class, cursor::advance);
    }

    @Test
    public void testCursor_key1() {
        final long key1 = randomKey();
        final long key2 = randomKey();
        hsa.ensure(key1, key2);

        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(key1, cursor.key1());
    }

    @Test
    public void testCursor_key2() {
        final long key1 = randomKey();
        final long key2 = randomKey();
        hsa.ensure(key1, key2);

        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(key2, cursor.key2());
    }

    @Test
    public void testCursor_valueAddress() {
        final SlotAssignmentResult slot = hsa.ensure(randomKey(), randomKey());

        HashSlotCursor16byteKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(slot.address(), cursor.valueAddress());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_advance_whenDisposed() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.advance();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_key1_whenDisposed() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.key1();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_key2_whenDisposed() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.key2();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_valueAddress_whenDisposed() {
        HashSlotCursor16byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.valueAddress();
    }

    @Test
    public void testCursor_withManyValues() {
        final long factor = 123456;
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            hsa.ensure(key1, key2);
        }

        boolean[] verifyKeys = new boolean[k];
        Arrays.fill(verifyKeys, false);

        HashSlotCursor16byteKey cursor = hsa.cursor();
        while (cursor.advance()) {
            long key1 = cursor.key1();
            long key2 = cursor.key2();

            assertEquals(key1 * factor, key2);
            verifyKeys[((int) key1) - 1] = true;
        }

        for (int i = 0; i < k; i++) {
            assertTrue("Haven't read " + k + "th key!", verifyKeys[i]);
        }
    }

    private long randomKey() {
        return random.nextInt(Integer.MAX_VALUE) + 1;
    }
}
