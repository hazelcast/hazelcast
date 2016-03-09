package com.hazelcast.spi.hashslot;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.memory.HeapMemoryManager;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashSlotArrayTwinKeyImplTest {

    // Value length must be at least 16 bytes, as required by the test's logic
    private static final int VALUE_LENGTH = 32;

    private final Random random = new Random();
    private MemoryManager memMgr;
    private MemoryAccessor mem;
    private HashSlotArrayTwinKey hsa;

    @Before
    public void setUp() throws Exception {
        memMgr = new HeapMemoryManager(32 << 20);
        mem = memMgr.getAccessor();
        hsa = new HashSlotArrayTwinKeyImpl(0L, memMgr, VALUE_LENGTH);
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
        final long valueAddress = insert(key1, key2);

        assertEquals(-valueAddress, hsa.ensure(key1, key2));
    }

    @Test
    public void testGet() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();
        final long valueAddress = insert(key1, key2);

        final long valueAddress2 = hsa.get(key1, key2);
        assertEquals(valueAddress, valueAddress2);
    }

    @Test
    public void testRemove() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();
        insert(key1, key2);

        assertTrue(hsa.remove(key1, key2));
        assertFalse(hsa.remove(key1, key2));
    }

    @Test
    public void testSize() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();

        insert(key1, key2);
        assertEquals(1, hsa.size());

        assertTrue(hsa.remove(key1, key2));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testClear() throws Exception {
        final long key1 = randomKey();
        final long key2 = randomKey();

        insert(key1, key2);
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
            insert(key1, key2);
        }

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = hsa.get(key1, key2);

            assertEquals(key1, mem.getLong(valueAddress));
            assertEquals(key2, mem.getLong(valueAddress + 8L));
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
            insert(key1, key2);
        }

        for (int i = mod; i <= k ; i += mod) {
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
                verifyValue(key1, key2, valueAddress);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPut_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.ensure(1, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testGet_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.get(1, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testRemove_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.remove(1, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testClear_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.clear();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key1_withoutAdvance() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.key1();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key2_withoutAdvance() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.key2();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_valueAddress_withoutAdvance() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.valueAddress();
    }

    @Test
    public void testCursor_advance_whenEmpty() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance() {
        insert(randomKey(), randomKey());

        HashSlotCursorTwinKey cursor = hsa.cursor();
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance_afterAdvanceReturnsFalse() {
        insert(randomKey(), randomKey());

        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.advance();
        cursor.advance();

        try {
            cursor.advance();
            fail("cursor.advance() should throw IllegalStateException, because previous advance() returned false!");
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testCursor_key1() {
        final long key1 = randomKey();
        final long key2 = randomKey();
        insert(key1, key2);

        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(key1, cursor.key1());
    }

    @Test
    public void testCursor_key2() {
        final long key1 = randomKey();
        final long key2 = randomKey();
        insert(key1, key2);

        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(key2, cursor.key2());
    }

    @Test
    public void testCursor_valueAddress() {
        final long valueAddress = insert(randomKey(), randomKey());

        HashSlotCursorTwinKey cursor = hsa.cursor();
        cursor.advance();
        assertEquals(valueAddress, cursor.valueAddress());
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_advance_whenDisposed() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.advance();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key1_whenDisposed() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.key1();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key2_whenDisposed() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
        hsa.dispose();
        cursor.key2();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_valueAddress_whenDisposed() {
        HashSlotCursorTwinKey cursor = hsa.cursor();
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
            long valueAddress = insert(key1, key2);
        }

        boolean[] verifyKeys = new boolean[k];
        Arrays.fill(verifyKeys, false);

        HashSlotCursorTwinKey cursor = hsa.cursor();
        while (cursor.advance()) {
            long key1 = cursor.key1();
            long key2 = cursor.key2();
            long valueAddress = cursor.valueAddress();

            assertEquals(key1 * factor, key2);
            verifyValue(key1, key2, valueAddress);

            verifyKeys[((int) key1) - 1] = true;
        }

        for (int i = 0; i < k; i++) {
            assertTrue("Haven't read " + k + "th key!", verifyKeys[i]);
        }
    }

    private long randomKey() {
        return random.nextInt(Integer.MAX_VALUE) + 1;
    }

    private long insert(long key1, long key2) {
        final long valueAddress = hsa.ensure(key1, key2);
        assertTrue(valueAddress > 0);
        // Value length must be at least 16 bytes
        mem.putLong(valueAddress, key1);
        mem.putLong(valueAddress + 8L, key2);
        return valueAddress;
    }

    private void verifyValue(long key1, long key2, long valueAddress) {
        // pre-check to avoid SIGSEGV
        assertNotEquals(NULL_ADDRESS, valueAddress);
        assertEquals(key1, mem.getLong(valueAddress));
        assertEquals(key2, mem.getLong(valueAddress + 8L));
    }
}
