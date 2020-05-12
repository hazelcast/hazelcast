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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PortableUtilsTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testConstructor() {
        assertUtilityConstructor(PortableUtils.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateAndGetArrayQuantifierFromCurrentToken_malformed() {
        PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs[", "person.legs[0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateAndGetArrayQuantifierFromCurrentToken_negative() {
        assertEquals(0, PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs[-1]", "person.legs[-1]"));
    }

    @Test
    public void validateAndGetArrayQuantifierFromCurrentToken_correct() {
        assertEquals(0, PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs[0]", "person.legs[0]"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateAndGetArrayQuantifierFromCurrentToken_withException() {
        PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs", null);
    }

    @Test
    public void getPortableArrayCellPosition() throws Exception {
        // GIVEN
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);
        int offset = 10;
        int cellIndex = 3;

        // WHEN
        PortableUtils.getPortableArrayCellPosition(in, offset, cellIndex);

        // THEN
        verify(in, times(1)).readInt(offset + cellIndex * Bits.INT_SIZE_IN_BYTES);
    }

    @Test
    public void isCurrentPathTokenWithoutQuantifier() {
        assertTrue(PortableUtils.isCurrentPathTokenWithoutQuantifier("wheels"));
        assertFalse(PortableUtils.isCurrentPathTokenWithoutQuantifier("wheels[1]"));
        assertFalse(PortableUtils.isCurrentPathTokenWithoutQuantifier("wheels[any]"));
    }

    @Test
    public void isCurrentPathTokenWithAnyQuantifier() {
        assertFalse(PortableUtils.isCurrentPathTokenWithAnyQuantifier("wheels"));
        assertFalse(PortableUtils.isCurrentPathTokenWithAnyQuantifier("wheels[1]"));
        assertTrue(PortableUtils.isCurrentPathTokenWithAnyQuantifier("wheels[any]"));
    }

    @Test
    public void unknownFieldException() {
        // GIVEN
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);
        ClassDefinition cd = mock(ClassDefinition.class);
        PortableNavigatorContext ctx = new PortableNavigatorContext(in, cd, null);

        // WHEN
        HazelcastSerializationException ex = PortableUtils.createUnknownFieldException(ctx, "person.brain");

        // THEN
        assertNotNull(ex);
    }

    @Test
    public void wrongUseOfAnyOperationException() {
        // GIVEN
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);
        ClassDefinition cd = mock(ClassDefinition.class);
        PortableNavigatorContext ctx = new PortableNavigatorContext(in, cd, null);

        // WHEN
        IllegalArgumentException ex = PortableUtils.createWrongUseOfAnyOperationException(ctx, "person.brain");

        // THEN
        assertNotNull(ex);
    }

    @Test
    public void validateArrayType_notArray() {
        // GIVEN
        ClassDefinition cd = mock(ClassDefinition.class);
        FieldDefinition fd = mock(FieldDefinition.class);

        // WHEN
        when(fd.getType()).thenReturn(FieldType.BOOLEAN);

        // THEN - ex thrown
        expected.expect(IllegalArgumentException.class);
        PortableUtils.validateArrayType(cd, fd, "person.brain");
    }

    @Test
    public void validateArrayType_array() {
        // GIVEN
        ClassDefinition cd = mock(ClassDefinition.class);
        FieldDefinition fd = mock(FieldDefinition.class);

        // WHEN
        when(fd.getType()).thenReturn(FieldType.BOOLEAN_ARRAY);

        // THEN - nothing thrown
        PortableUtils.validateArrayType(cd, fd, "person.brain");
    }

    @Test
    public void validateFactoryAndClass_compatible() {
        // GIVEN
        int factoryId = 1;
        int classId = 2;
        FieldDefinition fd = mock(FieldDefinition.class);

        // WHEN ids are compatible
        when(fd.getFactoryId()).thenReturn(factoryId);
        when(fd.getClassId()).thenReturn(classId);

        // THEN - nothing thrown
        PortableUtils.validateFactoryAndClass(fd, factoryId, classId, "person.brain");
    }

    @Test
    public void validateFactoryAndClass_incompatibleFactoryId() {
        // GIVEN
        int factoryId = 1;
        int classId = 2;
        FieldDefinition fd = mock(FieldDefinition.class);

        // WHEN ids are compatible
        when(fd.getFactoryId()).thenReturn(factoryId + 1);
        when(fd.getClassId()).thenReturn(classId);

        // THEN - ex thrown
        expected.expect(IllegalArgumentException.class);
        PortableUtils.validateFactoryAndClass(fd, factoryId, classId, "person.brain");
    }

    @Test
    public void validateFactoryAndClass_incompatibleClassId() {
        // GIVEN
        int factoryId = 1;
        int classId = 2;
        FieldDefinition fd = mock(FieldDefinition.class);

        // WHEN ids are compatible
        when(fd.getFactoryId()).thenReturn(factoryId);
        when(fd.getClassId()).thenReturn(classId + 1);

        // THEN - ex thrown
        expected.expect(IllegalArgumentException.class);
        PortableUtils.validateFactoryAndClass(fd, factoryId, classId, "person.brain");
    }
}
