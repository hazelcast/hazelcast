package com.hazelcast.internal.serialization.impl;

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
        assertEquals(1, cursor.length());
        assertEquals("engine", cursor.token());
        assertEquals("engine", cursor.path());
        assertTrue(cursor.isLastToken());
        assertFalse(cursor.isAnyPath());
        assertFalse(cursor.advanceToNextToken());
    }

    @Test
    public void multiElementToken_iterationOverAll() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.turbocharger.nozzle");

        // THEN - generic options
        assertEquals(3, cursor.length());
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

        // THEN - second token
        cursor.index(2);
        assertEquals("nozzle", cursor.token());
        assertTrue(cursor.isLastToken());

        // THEN - first token
        cursor.index(0);
        assertEquals("engine", cursor.token());
        assertFalse(cursor.isLastToken());

        // THEN - third token
        cursor.index(1);
        assertEquals("turbocharger", cursor.token());
        assertFalse(cursor.isLastToken());
    }

    @Test
    public void multiElementToken_jumpingToIndexOutOfBound() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();
        cursor.init("engine.turbocharger.nozzle");

        // WHEN
        cursor.index(3);

        // THEN
        expected.expect(ArrayIndexOutOfBoundsException.class);
        cursor.token();
    }

    @Test
    public void reuseOfCursor() {
        // GIVEN
        PortablePathCursor cursor = new PortablePathCursor();

        // THEN
        cursor.init("engine.turbocharger.nozzle");
        assertEquals(3, cursor.length());
        assertFalse(cursor.isAnyPath());
        assertEquals("engine", cursor.token());

        cursor.init("person.brain[any]");
        assertEquals(2, cursor.length());
        assertTrue(cursor.isAnyPath());
        assertEquals("person", cursor.token());
    }

}