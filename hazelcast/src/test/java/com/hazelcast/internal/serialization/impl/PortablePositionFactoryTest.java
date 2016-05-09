package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PortablePositionFactoryTest {

    @Test
    public void nilNonAnyPosition_nonLeaf() throws Exception {
        PortablePosition p = PortablePositionFactory.nilNotLeafPosition();

        assertTrue(p.isNull());
        assertFalse(p.isEmpty());
        assertFalse(p.isAny());
        assertFalse(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilNonAnyPosition_nonLeaf_cached() throws Exception {
        PortablePosition p1 = PortablePositionFactory.nilNotLeafPosition();
        PortablePosition p2 = PortablePositionFactory.nilNotLeafPosition();
        assertSame(p1, p2);
    }

    @Test
    public void nilAnyPosition_leaf() throws Exception {
        PortablePosition p = PortablePositionFactory.nilAnyPosition(true);

        assertTrue(p.isNull());
        assertFalse(p.isEmpty());
        assertTrue(p.isAny());
        assertTrue(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilAnyPositionNonLeaf_cached() throws Exception {
        PortablePosition p1 = PortablePositionFactory.nilAnyPosition(false);
        PortablePosition p2 = PortablePositionFactory.nilAnyPosition(false);
        assertSame(p1, p2);
    }

    @Test
    public void nilAnyPositionLeaf_cached() throws Exception {
        PortablePosition p1 = PortablePositionFactory.nilAnyPosition(true);
        PortablePosition p2 = PortablePositionFactory.nilAnyPosition(true);
        assertSame(p1, p2);
    }

    @Test
    public void nilAnyPosition_nonLeaf() throws Exception {
        PortablePosition p = PortablePositionFactory.nilAnyPosition(false);

        assertTrue(p.isNull());
        assertFalse(p.isEmpty());
        assertTrue(p.isAny());
        assertFalse(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void emptyAnyPosition_leaf() throws Exception {
        PortablePosition p = PortablePositionFactory.emptyAnyPosition(true);

        assertFalse(p.isNull());
        assertTrue(p.isEmpty());
        assertTrue(p.isAny());
        assertTrue(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void emptyAnyPosition_nonLeaf() throws Exception {
        PortablePosition p = PortablePositionFactory.emptyAnyPosition(false);

        assertTrue(p.isNull()); // automatically nullified, since empty and nonLeaf
        assertTrue(p.isEmpty());
        assertTrue(p.isAny());
        assertFalse(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void emptyAnyPositionNonLeaf_cached() throws Exception {
        PortablePosition p1 = PortablePositionFactory.emptyAnyPosition(false);
        PortablePosition p2 = PortablePositionFactory.emptyAnyPosition(false);
        assertSame(p1, p2);
    }

    @Test
    public void emptyAnyPositionLeaf_cached() throws Exception {
        PortablePosition p1 = PortablePositionFactory.emptyAnyPosition(true);
        PortablePosition p2 = PortablePositionFactory.emptyAnyPosition(true);
        assertSame(p1, p2);
    }

    @Test
    public void nilNotLeaf() throws Exception {
        PortablePosition p = PortablePositionFactory.nil(false);

        assertTrue(p.isNull());
        assertFalse(p.isLeaf());
        assertFalse(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilLeaf() throws Exception {
        PortablePosition p = PortablePositionFactory.nil(true);

        assertTrue(p.isNull());
        assertTrue(p.isLeaf());
        assertFalse(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilNotLeaf_any() throws Exception {
        PortablePosition p = PortablePositionFactory.nil(true, true);

        assertTrue(p.isNull());
        assertTrue(p.isLeaf());
        assertTrue(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilLeaf_any() throws Exception {
        PortablePosition p = PortablePositionFactory.nil(false, true);

        assertTrue(p.isNull());
        assertFalse(p.isLeaf());
        assertTrue(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void empty_nonLeaf_nonAny() {
        PortablePosition p = PortablePositionFactory.empty(false, false);

        assertTrue(p.isEmpty());
        assertTrue(p.isNull()); // nullified automatically

        assertFalse(p.isLeaf());
        assertFalse(p.isAny());
        assertEquals(0, p.getLen());
    }

    @Test
    public void empty_nonLeaf_any() {
        PortablePosition p = PortablePositionFactory.empty(false, true);

        assertTrue(p.isEmpty());
        assertTrue(p.isNull()); // nullified automatically

        assertFalse(p.isLeaf());
        assertTrue(p.isAny());
        assertEquals(0, p.getLen());
    }

    @Test
    public void empty_leaf_nonAny() {
        PortablePosition p = PortablePositionFactory.empty(true, false);

        assertTrue(p.isEmpty());
        assertFalse(p.isNull());

        assertTrue(p.isLeaf());
        assertFalse(p.isAny());
        assertEquals(0, p.getLen());
    }

    @Test
    public void empty_leaf_any() {
        PortablePosition p = PortablePositionFactory.empty(true, true);

        assertTrue(p.isEmpty());
        assertFalse(p.isNull());

        assertTrue(p.isLeaf());
        assertTrue(p.isAny());
        assertEquals(0, p.getLen());
    }

    @Test
    public void createSinglePrimitivePosition() throws Exception {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE);
        int streamPosition = 100;
        int index = 1;
        boolean leaf = true;

        // WHEN
        PortablePosition p = PortablePositionFactory.createSinglePrimitivePosition(fd, streamPosition, index, leaf);

        // THEN
        assertFalse(p.isNull());
        assertFalse(p.isEmpty());
        assertFalse(p.isNullOrEmpty());

        assertEquals(leaf, p.isLeaf());
        assertFalse(p.isAny());

        assertEquals(fd.getType(), p.getType());

        assertEquals(-1, p.getLen());
        assertEquals(-1, p.getClassId());
        assertEquals(-1, p.getFactoryId());

        assertEquals(index, p.getIndex());
        assertEquals(streamPosition, p.getStreamPosition());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void createSinglePortablePosition() throws Exception {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE);
        int streamPosition = 100;
        int factoryId = 123, classId = 546;
        boolean nil = false, leaf = true;

        // WHEN
        PortablePosition p = PortablePositionFactory.createSinglePortablePosition(fd, streamPosition, factoryId,
                classId, nil, leaf);

        // THEN
        assertFalse(p.isNull());
        assertFalse(p.isEmpty());
        assertFalse(p.isNullOrEmpty());

        assertEquals(nil, p.isNull());
        assertEquals(leaf, p.isLeaf());
        assertFalse(p.isAny());

        assertEquals(fd.getType(), p.getType());

        assertEquals(-1, p.getLen());
        assertEquals(classId, p.getClassId());
        assertEquals(factoryId, p.getFactoryId());

        assertEquals(-1, p.getIndex());
        assertEquals(streamPosition, p.getStreamPosition());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void createSinglePortablePosition_withIndex() throws Exception {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE);
        int streamPosition = 100;
        int factoryId = 123, classId = 546;
        int index = 27, len = 30;
        boolean leaf = true;

        // WHEN
        PortablePosition p = PortablePositionFactory.createSinglePortablePosition(fd, streamPosition, factoryId,
                classId, index, len, leaf);

        // THEN
        assertFalse(p.isNull());
        assertFalse(p.isEmpty());
        assertFalse(p.isNullOrEmpty());

        assertFalse(p.isNull());
        assertEquals(leaf, p.isLeaf());
        assertFalse(p.isAny());

        assertEquals(fd.getType(), p.getType());

        assertEquals(len, p.getLen());
        assertEquals(classId, p.getClassId());
        assertEquals(factoryId, p.getFactoryId());

        assertEquals(index, p.getIndex());
        assertEquals(streamPosition, p.getStreamPosition());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void createSinglePortablePosition_withIndex_nullifiedDueIndexOutOfBound() throws Exception {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE);
        int streamPosition = 100;
        int factoryId = 123, classId = 546;
        int index = 1, len = 0;
        boolean leaf = true;

        // WHEN
        PortablePosition p = PortablePositionFactory.createSinglePortablePosition(fd, streamPosition, factoryId,
                classId, index, len, leaf);

        // THEN
        assertTrue(p.isNull()); // nullified!
    }

    @Test
    public void createMultiPosition_withOnePosition() throws Exception {
        // GIVEN
        PortablePosition p = PortablePositionFactory.nilNotLeafPosition();

        // WHEN
        PortablePosition m = PortablePositionFactory.createMultiPosition(p);

        // THEN
        assertTrue(m.isMultiPosition());
        assertEquals(1, m.asMultiPosition().size());
        assertEquals(p, m.asMultiPosition().get(0));
    }

    @Test
    public void createMultiPosition_withMorePositions() throws Exception {
        // GIVEN
        PortablePosition p1 = PortablePositionFactory.nilNotLeafPosition();
        PortablePosition p2 = PortablePositionFactory.nil(true);

        // WHEN
        PortablePosition m = PortablePositionFactory.createMultiPosition(asList(p1, p2));

        // THEN
        assertTrue(m.isMultiPosition());
        assertEquals(2, m.asMultiPosition().size());
        assertEquals(p1, m.asMultiPosition().get(0));
        assertEquals(p2, m.asMultiPosition().get(1));
    }

}