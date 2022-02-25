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

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.util.collection.HsaHeapMemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashSlotArray8byteKeyImplTest {

    private static final int VALUE_LENGTH = 32;

    private final Random random = new Random();
    private HsaHeapMemoryManager memMgr;
    private MemoryAccessor mem;
    private HashSlotArray8byteKey hsa;

    @Before
    public void setUp() throws Exception {
        memMgr = new HsaHeapMemoryManager();
        mem = memMgr.getAccessor();
        hsa = new HashSlotArray8byteKeyImpl(0L, memMgr, VALUE_LENGTH);
        hsa.gotoNew();
    }

    @After
    public void tearDown() throws Exception {
        hsa.dispose();
        memMgr.dispose();
    }

    @Test
    public void testPut() throws Exception {
        final long key = random.nextLong();
        SlotAssignmentResult slot = insert(key);
        final long valueAddress = slot.address();
        assertTrue(slot.isNew());

        slot = hsa.ensure(key);
        assertFalse(slot.isNew());
        assertEquals(valueAddress, slot.address());
    }

    @Test
    public void testGet() throws Exception {
        final long key = random.nextLong();
        final long valueAddress = insert(key).address();

        final long valueAddress2 = hsa.get(key);
        assertEquals(valueAddress, valueAddress2);
    }

    @Test
    public void testRemove() throws Exception {
        final long key = random.nextLong();
        insert(key);

        assertTrue(hsa.remove(key));
        assertFalse(hsa.remove(key));
    }

    @Test
    public void testSize() throws Exception {
        final long key = random.nextLong();

        insert(key);
        assertEquals(1, hsa.size());

        assertTrue(hsa.remove(key));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testClear() throws Exception {
        final long key = random.nextLong();

        insert(key);
        hsa.clear();

        assertEquals(NULL_ADDRESS, hsa.get(key));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testPutGetMany() {
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            long valueAddress = hsa.get(key);

            verifyValue(key, valueAddress);
        }
    }

    @Test
    public void testPutRemoveGetMany() {
        final int k = 5000;
        final int mod = 100;

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }

        for (int i = mod; i <= k; i += mod) {
            long key = (long) i;
            assertTrue(hsa.remove(key));
        }

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            long valueAddress = hsa.get(key);

            if (i % mod == 0) {
                assertEquals(NULL_ADDRESS, valueAddress);
            } else {
                verifyValue(key, valueAddress);
            }
        }
    }

    @Test
    public void testMemoryNotLeaking() {
        final long k = 2000;
        for (long i = 1; i <= k; i++) {
            insert(i);
        }
        hsa.dispose();
        assertEquals("Memory leak: used memory not zero after dispose", 0, memMgr.getUsedMemory());

    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testPut_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.ensure(1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGet_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.get(1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testRemove_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.remove(1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testClear_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.clear();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_key_withoutAdvance() {
        HashSlotCursor8byteKey cursor = hsa.cursor();
        cursor.key();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_valueAddress_withoutAdvance() {
        HashSlotCursor8byteKey cursor = hsa.cursor();
        cursor.valueAddress();
    }

    @Test
    public void testCursor_advance_whenEmpty() {
        HashSlotCursor8byteKey cursor = hsa.cursor();
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance() {
        insert(random.nextLong());

        HashSlotCursor8byteKey cursor = hsa.cursor();
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
    }

    @Test
    @RequireAssertEnabled
    public void testCursor_advance_afterAdvanceReturnsFalse() {
        insert(random.nextLong());

        HashSlotCursor8byteKey cursor = hsa.cursor();
        cursor.advance();
        cursor.advance();

        assertThrows(AssertionError.class, cursor::advance);
    }

    @Test
    public void testCursor_key() {
        final long key = random.nextLong();
        insert(key);

        HashSlotCursor8byteKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(key, cursor.key());
    }

    @Test
    public void testCursor_valueAddress() {
        final SlotAssignmentResult slot = insert(random.nextLong());

        HashSlotCursor8byteKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(slot.address(), cursor.valueAddress());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_advance_whenDisposed() {
        HashSlotCursor8byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.advance();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_key_whenDisposed() {
        HashSlotCursor8byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.key();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCursor_valueAddress_whenDisposed() {
        HashSlotCursor8byteKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.valueAddress();
    }

    @Test
    public void testCursor_withManyValues() {
        final int k = 1000;
        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }
        boolean[] verifiedKeys = new boolean[k];
        HashSlotCursor8byteKey cursor = hsa.cursor();
        while (cursor.advance()) {
            long key = cursor.key();
            long valueAddress = cursor.valueAddress();
            verifyValue(key, valueAddress);
            verifiedKeys[((int) key) - 1] = true;
        }
        for (int i = 0; i < k; i++) {
            assertTrue("Failed to encounter key " + i, verifiedKeys[i]);
        }
    }

    private SlotAssignmentResult insert(long key) {
        final SlotAssignmentResult slot = hsa.ensure(key);
        assertNotEquals(NULL_ADDRESS, slot.address());
        mem.putLong(slot.address(), key);
        return slot;
    }

    private void verifyValue(long key, long valueAddress) {
        assertNotEquals("NULL value at key " + key, NULL_ADDRESS, valueAddress);
        assertEquals(key, mem.getLong(valueAddress));
    }
}
