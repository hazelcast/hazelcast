package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PortableUtilsTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void validateAndGetArrayQuantifierFromCurrentToken_malformed() throws Exception {
        PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs[", "person.legs[0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateAndGetArrayQuantifierFromCurrentToken_negtive() throws Exception {
        assertEquals(0, PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs[-1]", "person.legs[-1]"));
    }

    @Test
    public void validateAndGetArrayQuantifierFromCurrentToken_correct() throws Exception {
        assertEquals(0, PortableUtils.validateAndGetArrayQuantifierFromCurrentToken("legs[0]", "person.legs[0]"));
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
    public void isCurrentPathTokenWithoutQuantifier() throws Exception {
        assertTrue(PortableUtils.isCurrentPathTokenWithoutQuantifier("wheels"));
        assertFalse(PortableUtils.isCurrentPathTokenWithoutQuantifier("wheels[1]"));
        assertFalse(PortableUtils.isCurrentPathTokenWithoutQuantifier("wheels[any]"));
    }

    @Test
    public void isCurrentPathTokenWithAnyQuantifier() throws Exception {
        assertFalse(PortableUtils.isCurrentPathTokenWithAnyQuantifier("wheels"));
        assertFalse(PortableUtils.isCurrentPathTokenWithAnyQuantifier("wheels[1]"));
        assertTrue(PortableUtils.isCurrentPathTokenWithAnyQuantifier("wheels[any]"));
    }

    @Test
    public void unknownFieldException() throws Exception {
        // GIVEN
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);
        ClassDefinition cd = mock(ClassDefinition.class);
        PortableNavigatorContext ctx = new PortableNavigatorContext(in, cd, null);

        // WHEN
        Exception ex = PortableUtils.createUnknownFieldException(ctx, "person.brain");

        // THEN
        assertTrue(ex instanceof HazelcastSerializationException);
    }

    @Test
    public void wrongUseOfAnyOperationException() throws Exception {
        // GIVEN
        BufferObjectDataInput in = mock(BufferObjectDataInput.class);
        ClassDefinition cd = mock(ClassDefinition.class);
        PortableNavigatorContext ctx = new PortableNavigatorContext(in, cd, null);

        // WHEN
        Exception ex = PortableUtils.createWrongUseOfAnyOperationException(ctx, "person.brain");

        // THEN
        assertTrue(ex instanceof IllegalArgumentException);
    }

    @Test
    public void validateArrayType_notArray() throws Exception {
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
    public void validateArrayType_array() throws Exception {
        // GIVEN
        ClassDefinition cd = mock(ClassDefinition.class);
        FieldDefinition fd = mock(FieldDefinition.class);

        // WHEN
        when(fd.getType()).thenReturn(FieldType.BOOLEAN_ARRAY);

        // THEN - nothing thrown
        PortableUtils.validateArrayType(cd, fd, "person.brain");
    }

    @Test
    public void validateFactoryAndClass_compatible() throws Exception {
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
    public void validateFactoryAndClass_incompatibleFactoryId() throws Exception {
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
    public void validateFactoryAndClass_incompatibleClassId() throws Exception {
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