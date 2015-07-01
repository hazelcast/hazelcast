package com.hazelcast.hibernate.serialization;

import org.hibernate.internal.util.compare.ComparableComparator;
import org.junit.Test;

import static org.junit.Assert.*;

public class ValueTest {

    @Test
    public void testGetValue() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertEquals(expectedValue, value.getValue());
    }

    @Test
    public void testGetValueWithTimestampBefore() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertNull(value.getValue(99L));
    }

    @Test
    public void testGetValueWithTimestampEqual() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertEquals(expectedValue, value.getValue(100L));
    }

    @Test
    public void testGetValueWithTimestampAfter() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertEquals(expectedValue, value.getValue(101L));
    }

    @Test
    public void testIsReplaceableByTimestampBefore() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertFalse(value.isReplaceableBy(99L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampEqual() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertTrue(value.isReplaceableBy(100L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampAfter() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(null, 100L, expectedValue);
        assertTrue(value.isReplaceableBy(101L, null, null));
    }

    @Test
    public void testIsReplaceableByVersionBefore() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(10, 100L, expectedValue);
        assertFalse(value.isReplaceableBy(99L, 9, ComparableComparator.INSTANCE));
    }

    @Test
    public void testIsReplaceableByVersionEqual() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(10, 100L, expectedValue);
        assertFalse(value.isReplaceableBy(101L, 10, ComparableComparator.INSTANCE));
    }

    @Test
    public void testIsReplaceableByVersionAfter() throws Exception {
        String expectedValue = "Some value";
        Value value = new Value(10, 100L, expectedValue);
        assertTrue(value.isReplaceableBy(99L, 11, ComparableComparator.INSTANCE));
    }

}