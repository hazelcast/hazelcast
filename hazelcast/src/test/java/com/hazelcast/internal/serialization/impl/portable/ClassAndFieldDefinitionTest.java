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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClassAndFieldDefinitionTest {

    int portableVersion = 1;
    private ClassDefinitionImpl classDefinition;
    private static String[] fieldNames = new String[]{"f1", "f2", "f3"};

    @Before
    public void setUp() throws Exception {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(1, 2, 3);
        for (String fieldName : fieldNames) {
            builder.addByteField(fieldName);
        }
        classDefinition = (ClassDefinitionImpl) builder.build();

    }

    @Test
    public void testClassDef_getter_setter() throws Exception {
        ClassDefinitionImpl cd = (ClassDefinitionImpl) new ClassDefinitionBuilder(1, 2, portableVersion).build();
        cd.setVersionIfNotSet(3);
        cd.setVersionIfNotSet(5);

        assertEquals(1, cd.getFactoryId());
        assertEquals(2, cd.getClassId());
        assertEquals(portableVersion, cd.getVersion());
        assertEquals(3, classDefinition.getFieldCount());
    }

    @Test
    public void testClassDef_getField_properIndex() throws Exception {
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition field = classDefinition.getField(i);
            assertNotNull(field);
        }
    }

    @Test
    public void testClassDef_hasField() throws Exception {
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            String fieldName = fieldNames[i];
            boolean hasField = classDefinition.hasField(fieldName);
            assertTrue(hasField);
        }
    }

    @Test
    public void testClassDef_getFieldType() throws Exception {
        for (String fieldName : fieldNames) {
            FieldType fieldType = classDefinition.getFieldType(fieldName);
            assertNotNull(fieldType);
        }
    }

    @Test
    public void testClassDef_getFieldClassId() throws Exception {
        for (String fieldName : fieldNames) {
            int classId = classDefinition.getFieldClassId(fieldName);
            assertEquals(0, classId);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClassDef_getFieldClassId_invalidField() throws Exception {
        classDefinition.getFieldClassId("The Invalid Field");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClassDef_getFieldType_invalidField() throws Exception {
        classDefinition.getFieldType("The Invalid Field");
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testClassDef_getField_negativeIndex() throws Exception {
        classDefinition.getField(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testClassDef_getField_HigherThenSizeIndex() throws Exception {
        classDefinition.getField(classDefinition.getFieldCount());
    }

    @Test
    public void testClassDef_equal_hashCode() throws Exception {
        ClassDefinitionImpl cdEmpty1 = (ClassDefinitionImpl) new ClassDefinitionBuilder(1, 2, 3).build();
        ClassDefinitionImpl cdEmpty2 = (ClassDefinitionImpl) new ClassDefinitionBuilder(1, 2, 3).build();
        ClassDefinitionImpl cd1 = (ClassDefinitionImpl) new ClassDefinitionBuilder(1, 2, 5).build();
        ClassDefinitionImpl cd2 = (ClassDefinitionImpl) new ClassDefinitionBuilder(2, 2, 3).build();
        ClassDefinitionImpl cd3 = (ClassDefinitionImpl) new ClassDefinitionBuilder(1, 9, 3).build();

        ClassDefinitionImpl cdWithField = (ClassDefinitionImpl) new ClassDefinitionBuilder(1, 2, 3).addIntField("f1").build();

        assertEquals(cdEmpty1, cdEmpty2);
        assertNotEquals(cd1, cdEmpty1);
        assertNotEquals(cd2, cdEmpty1);
        assertNotEquals(cd3, cdEmpty1);
        assertNotEquals(cdWithField, classDefinition);
        assertNotEquals(cdEmpty1, classDefinition);
        assertNotEquals(classDefinition, null);
        assertNotEquals(classDefinition, "Another Class");

        assertNotEquals(0, cd1.hashCode());
    }

    @Test
    public void testClassDef_toString() throws Exception {
        assertNotNull(classDefinition.toString());
    }

    @Test
    public void testFieldDef_getter_setter() throws Exception {
        FieldDefinition field0 = classDefinition.getField(0);
        FieldDefinition field = classDefinition.getField("f1");

        FieldDefinitionImpl fd = new FieldDefinitionImpl(9, "name", FieldType.PORTABLE, 5, 6, 7);
        FieldDefinitionImpl fd_nullName = new FieldDefinitionImpl(10, null, FieldType.PORTABLE, 15, 16, 17);

        assertEquals(field, field0);

        assertEquals(0, field.getFactoryId());
        assertEquals(0, field.getClassId());
        assertEquals(3, field.getVersion());
        assertEquals(0, field.getIndex());
        assertEquals("f1", field.getName());
        assertEquals(FieldType.BYTE, field.getType());

        assertEquals(5, fd.getFactoryId());
        assertEquals(6, fd.getClassId());
        assertEquals(7, fd.getVersion());
        assertEquals(9, fd.getIndex());
        assertEquals("name", fd.getName());
        assertEquals(FieldType.PORTABLE, fd.getType());

        assertEquals(15, fd_nullName.getFactoryId());
        assertEquals(16, fd_nullName.getClassId());
        assertEquals(17, fd_nullName.getVersion());
        assertEquals(10, fd_nullName.getIndex());
        assertNull(fd_nullName.getName());
        assertEquals(FieldType.PORTABLE, fd_nullName.getType());
    }

    @Test
    public void testFieldDef_equal_hashCode() throws Exception {
        FieldDefinitionImpl fd0 = new FieldDefinitionImpl(0, "name", FieldType.BOOLEAN, portableVersion);
        FieldDefinitionImpl fd0_1 = new FieldDefinitionImpl(0, "name", FieldType.INT, portableVersion);
        FieldDefinitionImpl fd1 = new FieldDefinitionImpl(1, "name", FieldType.BOOLEAN, portableVersion);
        FieldDefinitionImpl fd2 = new FieldDefinitionImpl(0, "namex", FieldType.BOOLEAN, portableVersion);

        assertNotEquals(fd0, fd0_1);
        assertNotEquals(fd0, fd1);
        assertNotEquals(fd0, fd2);
        assertNotEquals(fd0, null);
        assertNotEquals(fd0, "Another Class");

        assertNotEquals(0, fd0.hashCode());
    }

    @Test
    public void testFieldDef_toString() throws Exception {
        assertNotNull(new FieldDefinitionImpl(0, "name", FieldType.BOOLEAN, portableVersion).toString());
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testClassDefinitionBuilder_addingSameFieldTwice() {
        new ClassDefinitionBuilder(1, 2, 1)
                .addStringField("name").addStringField("name");
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testClassDefinitionBuilder_addingSameFieldTwice_withDifferentType() {
        new ClassDefinitionBuilder(1, 2, 1)
                .addStringField("name").addIntField("name");
    }
}
