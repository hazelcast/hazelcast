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

import com.hazelcast.internal.memory.impl.HeapMemoryManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class Long2LongMapHsaTest {

    private static final long MISSING_VALUE = -1L;

    private final Random random = new Random();
    private HeapMemoryManager memMgr;
    private Long2LongMapHsa map;

    @Before
    public void setUp() throws Exception {
        memMgr = new HeapMemoryManager(2 * 1000 * 1000);
        map = new Long2LongMapHsa(MISSING_VALUE, memMgr);
    }

    @After
    public void tearDown() throws Exception {
        map.dispose();
        memMgr.dispose();
    }

    @Test
    public void testPut() {
        long key = newKey();
        long value = newValue();
        assertEqualsKV(MISSING_VALUE, map.put(key, value), key, value);

        long newValue = newValue();
        long oldValue = map.put(key, newValue);
        assertEqualsKV(value, oldValue, key, value);
    }

    @Test
    public void testGet() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long currentValue = map.get(key);
        assertEqualsKV(value, currentValue, key, value);
    }

    @Test
    public void testPutIfAbsent_success() {
        long key = newKey();
        long value = newValue();
        assertEqualsKV(MISSING_VALUE, map.putIfAbsent(key, value), key, value);
    }

    @Test
    public void testPutIfAbsent_fail() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long newValue = newValue();
        assertEqualsKV(value, map.putIfAbsent(key, newValue), key, value);
    }

    @Test
    public void testPutAll() throws Exception {
        int count = 100;
        Long2LongMap entries = new Long2LongMapHsa(count, memMgr);

        for (int i = 0; i < count; i++) {
            long key = newKey();
            long value = newValue();
            entries.put(key, value);
        }

        map.putAll(entries);
        assertEquals(count, map.size());

        for (LongLongCursor cursor = map.cursor(); cursor.advance(); ) {
            assertEquals(map.get(cursor.key()), cursor.value());
        }
    }

    @Test
    public void testReplace() throws Exception {
        long key = newKey();
        long value = newValue();

        assertEqualsKV(MISSING_VALUE, map.replace(key, value), key, value);

        map.put(key, value);

        long newValue = newValue();
        assertEqualsKV(value, map.replace(key, newValue), key, value);
    }

    @Test
    public void testReplace_if_same_success() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long newValue = value + 1;
        assertTrueKV(map.replace(key, value, newValue), key, value);
    }

    @Test
    public void testReplace_if_same_not_exist() {
        long key = newKey();
        long value = newValue();
        long newValue = value + 1;
        assertFalseKV(map.replace(key, value, newValue), key, value);
    }

    @Test
    public void testReplace_if_same_fail() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long wrongValue = value + 1;
        long newValue = value + 2;
        assertFalseKV(map.replace(key, wrongValue, newValue), key, newValue);
    }

    @Test
    public void testRemove() {
        long key = newKey();
        assertEqualsKV(MISSING_VALUE, map.remove(key), key, 0);

        long value = newValue();
        map.put(key, value);

        long oldValue = map.remove(key);
        assertEqualsKV(value, oldValue, key, value);
    }

    @Test
    public void testRemove_if_same_success() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        assertTrueKV(map.remove(key, value), key, value);
    }

    @Test
    public void testRemove_if_same_not_exist() {
        long key = newKey();
        long value = newValue();
        assertFalseKV(map.remove(key, value), key, value);
    }

    @Test
    public void testRemove_if_same_fail() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long wrongValue = value + 1;
        assertFalseKV(map.remove(key, wrongValue), key, value);
    }

    @Test
    public void testContainsKey_fail() throws Exception {
        long key = newKey();
        assertFalseKV(map.containsKey(key), key, 0);
    }

    @Test
    public void testContainsKey_success() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        assertTrueKV(map.containsKey(key), key, value);
    }

    @Test
    public void testPut_withTheSameValue() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long oldValue = map.put(key, value);
        assertEqualsKV(value, oldValue, key, value);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_put_invalidValue() {
        map.put(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_putIfAbsent_invalidValue() {
        map.putIfAbsent(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_set_invalidValue() {
        map.put(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_replace_invalidValue() {
        map.replace(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_replaceIfEquals_invalidOldValue() {
        map.replace(newKey(), MISSING_VALUE, newValue());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_replaceIfEquals_invalidNewValue() {
        map.replace(newKey(), newValue(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void test_removeIfEquals_Value() {
        map.remove(newKey(), MISSING_VALUE);
    }

    @Test
    public void testSize() {
        assertEquals(0, map.size());

        int expected = 100;
        for (long i = 0; i < expected; i++) {
            long value = newValue();
            map.put(i, value);
        }

        assertEquals(map.toString(), expected, map.size());
    }

    @Test
    public void testClear() {
        for (long i = 0; i < 100; i++) {
            long value = newValue();
            map.put(i, value);
        }

        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.toString(), map.isEmpty());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(map.isEmpty());

        long key = newKey();
        long value = newValue();
        map.put(key, value);

        assertFalseKV(map.isEmpty(), key, value);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGet_after_dispose() {
        map.dispose();
        map.get(newKey());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testPut_after_dispose() {
        map.dispose();
        map.put(newKey(), newValue());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testRemove_after_dispose() {
        map.dispose();
        map.remove(newKey());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testReplace_after_dispose() {
        map.dispose();
        map.replace(newKey(), newValue());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testContainsKey_after_dispose() {
        map.dispose();
        map.containsKey(newKey());
    }

    @Test
    public void testMemoryLeak() {
        int keyRange = 100;

        for (int i = 0; i < 100000; i++) {
            int k = random.nextInt(7);
            switch (k) {
                case 0:
                    _put_(keyRange);
                    break;

                case 1:
                    _set_(keyRange);
                    break;

                case 2:
                    _putIfAbsent_(keyRange);
                    break;

                case 3:
                    _replace_(keyRange);
                    break;

                case 4:
                    _replaceIfSame_(keyRange);
                    break;

                case 5:
                    _remove_(keyRange);
                    break;

                case 6:
                    _removeIfPresent_(keyRange);
                    break;
            }
        }

        map.clear();
        map.dispose();
        assertEquals(0, memMgr.getUsedMemory());
    }

    private void _put_(int keyRange) {
        map.put(newKey(keyRange), newValue());
    }

    private void _set_(int keyRange) {
        map.put(newKey(keyRange), newValue());
    }

    private void _putIfAbsent_(int keyRange) {
        map.putIfAbsent(newKey(keyRange), newValue());
    }

    private void _replace_(int keyRange) {
        map.replace(newKey(keyRange), newValue());
    }

    private void _replaceIfSame_(int keyRange) {
        long key = newKey(keyRange);
        long value = newValue();
        long old = map.get(key);
        if (old != MISSING_VALUE) {
            map.replace(key, old, value);
        }
    }

    private void _remove_(int keyRange) {
        map.remove(newKey(keyRange));
    }

    private void _removeIfPresent_(int keyRange) {
        long key = newKey(keyRange);
        long old = map.get(key);
        if (old != MISSING_VALUE) {
            map.remove(key, old);
        }
    }

    @Test
    public void testDestroyMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.put(newKey(), newValue());
        }

        map.clear();
        map.dispose();
        assertEquals(0, memMgr.getUsedMemory());
    }

    @Test
    public void testMemoryLeak_whenCapacityExpandFails() {
        while (true) {
            long key = newKey();
            long value = newValue();
            try {
                map.put(key, value);
            } catch (OutOfMemoryError e) {
                break;
            }
        }

        map.clear();
        map.dispose();
        assertEquals(0, memMgr.getUsedMemory());
    }

    private long newKey() {
        return random.nextLong();
    }

    private long newKey(int keyRange) {
        return (long) random.nextInt(keyRange);
    }

    private long newValue() {
        return random.nextInt(Integer.MAX_VALUE) + 1L;
    }

    private static void assertEqualsKV(long expected, long actual, long key, long value) {
        Assert.assertEquals(String.format("key %d value %d", key, value), expected, actual);
    }

    private static void assertTrueKV(boolean actual, long key, long value) {
        Assert.assertTrue(String.format("key %d value %d", key, value), actual);
    }

    private static void assertFalseKV(boolean actual, long key, long value) {
        Assert.assertFalse(String.format("key %d value %d", key, value), actual);
    }
}
