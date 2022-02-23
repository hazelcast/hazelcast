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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonPathCursorTest {

    @Test
    public void testOneItemPath() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPathWithUnMatchedCloseBraceShouldThrowIllegalArgumentException() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("a]");
        cursor.getNext();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPathWithNonNumberArrayIndexShouldThrowIllegalArgumentException() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[a]");
        cursor.getNext();
    }

    @Test
    public void testOneItemArrayPath() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[4]");
        assertEquals("4", cursor.getNext());
        assertEquals(4, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoItemPath() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc.def");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("def", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoArrayItemPath() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[4][1]");
        assertEquals("4", cursor.getNext());
        assertEquals(4, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("1", cursor.getNext());
        assertEquals(1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoItemPath_whenTheSecondIsArray() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc[1]");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("1", cursor.getNext());
        assertEquals(1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoItemPath_whenTheFirstIsArray() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[1].abc");
        assertEquals("1", cursor.getNext());
        assertEquals(1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testOneItemArrayPath_whenAny() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[any]");
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoItemArrayPath_whenAny() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[any][any]");
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoItemPath_whenFirstIsAny() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[any].abc");
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testTwoItemPath_whenSecondIsAny() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc[any]");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenAllNonArray() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc.def.ghi");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("def", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("ghi", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenNonArray_array_any() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc[12][any]");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("12", cursor.getNext());
        assertEquals(12, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenNonArray_any_array() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("abc[any][12]");
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("12", cursor.getNext());
        assertEquals(12, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenArray_nonArray_any() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[12].abc[any]");
        assertEquals("12", cursor.getNext());
        assertEquals(12, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenArray_any_nonArray() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[12][any].abc");
        assertEquals("12", cursor.getNext());
        assertEquals(12, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenAny_nonArray_array() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[any].abc[12]");
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEquals("12", cursor.getNext());
        assertEquals(12, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEnd(cursor);
    }

    @Test
    public void testThreeItemPath_whenAny_array_nonArray() {
        JsonPathCursor cursor = JsonPathCursor.createCursor("[any][12].abc");
        assertEquals("any", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertTrue(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("12", cursor.getNext());
        assertEquals(12, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertTrue(cursor.isArray());
        assertEquals("abc", cursor.getNext());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isAny());
        assertFalse(cursor.isArray());
        assertEnd(cursor);
    }

    private void assertEnd(JsonPathCursor cursor) {
        assertNull(cursor.getNext());
        assertNull(cursor.getNext());
        assertNull(cursor.getCurrent());
        assertEquals(-1, cursor.getArrayIndex());
        assertFalse(cursor.isArray());
        assertFalse(cursor.isAny());
    }
}
