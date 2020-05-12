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

import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("ConstantConditions")
public class PortablePositionFactoryTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(PortablePositionFactory.class);
    }

    @Test
    public void nilNonAnyPosition_nonLeaf() {
        PortablePosition p = PortablePositionFactory.nilNotLeafPosition();

        assertTrue(p.isNull());
        assertFalse(p.isEmpty());
        assertFalse(p.isAny());
        assertFalse(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilNonAnyPosition_nonLeaf_cached() {
        PortablePosition p1 = PortablePositionFactory.nilNotLeafPosition();
        PortablePosition p2 = PortablePositionFactory.nilNotLeafPosition();
        assertSame(p1, p2);
    }

    @Test
    public void nilAnyPosition_leaf() {
        PortablePosition p = PortablePositionFactory.nilAnyPosition(true);

        assertTrue(p.isNull());
        assertFalse(p.isEmpty());
        assertTrue(p.isAny());
        assertTrue(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilAnyPositionNonLeaf_cached() {
        PortablePosition p1 = PortablePositionFactory.nilAnyPosition(false);
        PortablePosition p2 = PortablePositionFactory.nilAnyPosition(false);
        assertSame(p1, p2);
    }

    @Test
    public void nilAnyPositionLeaf_cached() {
        PortablePosition p1 = PortablePositionFactory.nilAnyPosition(true);
        PortablePosition p2 = PortablePositionFactory.nilAnyPosition(true);
        assertSame(p1, p2);
    }

    @Test
    public void nilAnyPosition_nonLeaf() {
        PortablePosition p = PortablePositionFactory.nilAnyPosition(false);

        assertTrue(p.isNull());
        assertFalse(p.isEmpty());
        assertTrue(p.isAny());
        assertFalse(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void emptyAnyPosition_leaf() {
        PortablePosition p = PortablePositionFactory.emptyAnyPosition(true);

        assertFalse(p.isNull());
        assertTrue(p.isEmpty());
        assertTrue(p.isAny());
        assertTrue(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void emptyAnyPosition_nonLeaf() {
        PortablePosition p = PortablePositionFactory.emptyAnyPosition(false);

        assertTrue(p.isNull()); // automatically nullified, since empty and nonLeaf
        assertTrue(p.isEmpty());
        assertTrue(p.isAny());
        assertFalse(p.isLeaf());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void emptyAnyPositionNonLeaf_cached() {
        PortablePosition p1 = PortablePositionFactory.emptyAnyPosition(false);
        PortablePosition p2 = PortablePositionFactory.emptyAnyPosition(false);
        assertSame(p1, p2);
    }

    @Test
    public void emptyAnyPositionLeaf_cached() {
        PortablePosition p1 = PortablePositionFactory.emptyAnyPosition(true);
        PortablePosition p2 = PortablePositionFactory.emptyAnyPosition(true);
        assertSame(p1, p2);
    }

    @Test
    public void nilNotLeaf() {
        PortablePosition p = PortablePositionFactory.nil(false);

        assertTrue(p.isNull());
        assertFalse(p.isLeaf());
        assertFalse(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilLeaf() {
        PortablePosition p = PortablePositionFactory.nil(true);

        assertTrue(p.isNull());
        assertTrue(p.isLeaf());
        assertFalse(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilNotLeaf_any() {
        PortablePosition p = PortablePositionFactory.nil(true, true);

        assertTrue(p.isNull());
        assertTrue(p.isLeaf());
        assertTrue(p.isAny());
        assertFalse(p.isMultiPosition());
    }

    @Test
    public void nilLeaf_any() {
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
    public void createSinglePrimitivePosition() {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE, 0);
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
        assertAsMultiPositionThrowsException(p);
    }

    @Test
    public void createSinglePrimitivePosition_withoutFieldDefinition() {
        // GIVEN
        int streamPosition = 100;
        int index = 1;
        boolean leaf = true;

        // WHEN
        PortablePosition p = PortablePositionFactory.createSinglePrimitivePosition(null, streamPosition, index, leaf);

        // THEN
        assertFalse(p.isNull());
        assertFalse(p.isEmpty());
        assertFalse(p.isNullOrEmpty());

        assertEquals(leaf, p.isLeaf());
        assertFalse(p.isAny());

        assertNull(p.getType());

        assertEquals(-1, p.getLen());
        assertEquals(-1, p.getClassId());
        assertEquals(-1, p.getFactoryId());

        assertEquals(index, p.getIndex());
        assertEquals(streamPosition, p.getStreamPosition());
        assertFalse(p.isMultiPosition());
        assertAsMultiPositionThrowsException(p);
    }

    @Test
    public void createSinglePortablePosition() {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE, 0);
        int streamPosition = 100;
        int factoryId = 123;
        int classId = 546;
        boolean nil = false;
        boolean leaf = true;

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
        assertAsMultiPositionThrowsException(p);
    }

    @Test
    public void createSinglePortablePosition_withIndex() {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE, 0);
        int streamPosition = 100;
        int factoryId = 123;
        int classId = 546;
        int index = 27;
        int len = 30;
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
    public void createSinglePortablePosition_withIndex_nullifiedDueIndexOutOfBound() {
        // GIVEN
        FieldDefinition fd = new FieldDefinitionImpl(1, "field", FieldType.PORTABLE, 0);
        int streamPosition = 100;
        int factoryId = 123;
        int classId = 546;
        int index = 1;
        int len = 0;
        boolean leaf = true;

        // WHEN
        PortablePosition p = PortablePositionFactory.createSinglePortablePosition(fd, streamPosition, factoryId,
                classId, index, len, leaf);

        // THEN
        assertTrue(p.isNull()); // nullified!
    }

    @Test
    public void createMultiPosition_withEmptyPositionList() {
        // GIVEN
        List<PortablePosition> list = emptyList();

        // WHEN
        PortablePosition m = PortablePositionFactory.createMultiPosition(list);

        // THEN
        assertTrue(m.isMultiPosition());
        assertEquals(0, m.asMultiPosition().size());
        assertNull(m.getType());
    }

    @Test
    public void createMultiPosition_withOnePosition() {
        // GIVEN
        PortablePosition p = PortablePositionFactory.nilNotLeafPosition();

        // WHEN
        PortablePosition m = PortablePositionFactory.createMultiPosition(p);

        // THEN
        assertTrue(m.isMultiPosition());
        assertEquals(1, m.asMultiPosition().size());
        assertEquals(p, m.asMultiPosition().get(0));
        assertEquals(p.getType(), m.getType());
    }

    @Test
    public void createMultiPosition_withMorePositions() {
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
        assertEquals(p1.getType(), m.getType());
    }

    private static void assertAsMultiPositionThrowsException(PortablePosition portablePosition) {
        try {
            portablePosition.asMultiPosition();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
    }
}
