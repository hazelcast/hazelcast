/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PortablePathCursorTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void nonInitialised_throwsException() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();

        // THEN
        expected.expect(NullPointerException.class);
        cursor.advanceToNextToken();
    }

    @Test
    public void initialised_reset_throwsException() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.oil");

        // WHEN
        cursor.reset();

        // THEN
        expected.expect(NullPointerException.class);
        cursor.advanceToNextToken();
    }

    @Test
    public void oneElementToken() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine");

        // THEN
        assertEquals("engine", cursor.token());
        assertEquals("engine", cursor.path());
        assertTrue(cursor.isLastToken());
        assertFalse(cursor.isAnyPath());
        assertFalse(cursor.advanceToNextToken());
        assertFalse(cursor.advanceToNextToken());
        assertEquals("engine", cursor.token());
    }

    @Test
    public void multiElementToken_iterationOverAll() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.turbocharger.nozzle");

        // THEN - generic options
        assertFalse(cursor.isAnyPath());
        assertEquals("engine.turbocharger.nozzle", cursor.path());

        // THEN - first token
        assertEquals("engine", cursor.token());
        assertFalse(cursor.isLastToken());

        // THEN - second token
        assertTrue(cursor.advanceToNextToken());
        assertEquals("turbocharger", cursor.token());
        assertFalse(cursor.isLastToken());

        // THEN - third token
        assertTrue(cursor.advanceToNextToken());
        assertEquals("nozzle", cursor.token());
        assertTrue(cursor.isLastToken());

        // THEN - no other token
        assertFalse(cursor.advanceToNextToken());
        assertEquals("nozzle", cursor.token());
    }

    @Test
    public void multiElementToken_anyPath() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.turbocharger[any].nozzle");

        // THEN
        assertTrue(cursor.isAnyPath());
    }

    @Test
    public void multiElementToken_jumpingToIndex() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.turbocharger.nozzle");

        // THEN - 2nd token
        cursor.index(2);
        assertEquals("nozzle", cursor.token());
        assertTrue(cursor.isLastToken());
        assertEquals(2, cursor.index());
        assertFalse(cursor.advanceToNextToken());

        // THEN - 1st token
        cursor.index(0);
        assertEquals("engine", cursor.token());
        assertFalse(cursor.isLastToken());
        assertEquals(0, cursor.index());
        assertTrue(cursor.advanceToNextToken());

        // THEN - 3rd token
        cursor.index(1);
        assertEquals("turbocharger", cursor.token());
        assertFalse(cursor.isLastToken());
        assertEquals(1, cursor.index());
        assertTrue(cursor.advanceToNextToken());
    }

    @Test
    public void multiElementToken_jumpingToIndexOutOfBound() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.turbocharger.nozzle");

        // WHEN
        expected.expect(IndexOutOfBoundsException.class);
        cursor.index(3);

        // THEN

        cursor.token();
    }

    @Test
    public void reuseOfCursor() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();

        // THEN
        cursor.init("engine.turbocharger.nozzle");
        assertFalse(cursor.isAnyPath());
        assertEquals("engine", cursor.token());

        cursor.init("person.brain[any]");
        assertTrue(cursor.isAnyPath());
        assertEquals("person", cursor.token());
    }

    @Test
    public void emptyPath() {
        PortablePathCursor cursor = new PortablePathCursor();

        expected.expect(IllegalArgumentException.class);
        cursor.init("");
    }

    @Test
    public void nullPath() {
        PortablePathCursor cursor = new PortablePathCursor();

        expected.expect(IllegalArgumentException.class);
        cursor.init(null);
    }

    @Test
    public void wrongPath_dotOnly() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();

        expected.expect(IllegalArgumentException.class);
        cursor.init(".");

        // THEN
        assertEquals("", cursor.token());
        assertEquals("", cursor.path());
        assertTrue(cursor.isLastToken());
        assertFalse(cursor.isAnyPath());
        assertFalse(cursor.advanceToNextToken());
    }

    @Test
    public void wrongPath_moreDots() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();

        expected.expect(IllegalArgumentException.class);
        cursor.init("...");
    }

    @Test
    public void wrongPath_emptyTokens() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("a..");

        // THEN - 1st token
        assertEquals("a", cursor.token());
        assertTrue(cursor.advanceToNextToken());

        // THEN - 2nd token
        assertTokenThrowsException(cursor);
        assertTrue(cursor.advanceToNextToken());

        // THEN - 3rd token
        assertTokenThrowsException(cursor);
        assertFalse(cursor.advanceToNextToken());
    }

    @Test
    public void wrongPath_pathEndingWithDot() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("a.b.");

        // THEN - 1st token
        assertEquals("a", cursor.token());
        assertTrue(cursor.advanceToNextToken());

        // THEN - 2nd token
        assertEquals("b", cursor.token());
        assertTrue(cursor.advanceToNextToken());

        // THEN - 3rd token
        assertTokenThrowsException(cursor);
        assertFalse(cursor.advanceToNextToken());
    }

    private static void assertTokenThrowsException(PortablePathCursor cursor) {
        try {
            assertEquals("", cursor.token());
            fail();
        } catch (IllegalArgumentException expected) {
        }
    }
}
