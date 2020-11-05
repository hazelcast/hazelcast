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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class PortableUpsertTargetTest {

    @Test
    public void test_set() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition innerClassDefinition = new ClassDefinitionBuilder(4, 5, 6).build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addPortableField("null", innerClassDefinition)
                        .addPortableField("object", innerClassDefinition)
                        .addUTFField("string")
                        .addCharField("character")
                        .addBooleanField("boolean")
                        .addByteField("byte")
                        .addShortField("short")
                        .addIntField("int")
                        .addLongField("long")
                        .addFloatField("float")
                        .addDoubleField("double")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector nullFieldInjector = target.createInjector("null", QueryDataType.OBJECT);
        UpsertInjector objectFieldInjector = target.createInjector("object", QueryDataType.OBJECT);
        UpsertInjector stringFieldInjector = target.createInjector("string", QueryDataType.VARCHAR);
        UpsertInjector characterFieldInjector = target.createInjector("character", QueryDataType.VARCHAR_CHARACTER);
        UpsertInjector booleanFieldInjector = target.createInjector("boolean", QueryDataType.BOOLEAN);
        UpsertInjector byteFieldInjector = target.createInjector("byte", QueryDataType.TINYINT);
        UpsertInjector shortFieldInjector = target.createInjector("short", QueryDataType.SMALLINT);
        UpsertInjector intFieldInjector = target.createInjector("int", QueryDataType.INT);
        UpsertInjector longFieldInjector = target.createInjector("long", QueryDataType.BIGINT);
        UpsertInjector floatFieldInjector = target.createInjector("float", QueryDataType.REAL);
        UpsertInjector doubleFieldInjector = target.createInjector("double", QueryDataType.DOUBLE);

        target.init();
        nullFieldInjector.set(null);
        objectFieldInjector.set(new PortableGenericRecordBuilder(innerClassDefinition).build());
        stringFieldInjector.set("1");
        characterFieldInjector.set('2');
        booleanFieldInjector.set(true);
        byteFieldInjector.set((byte) 3);
        shortFieldInjector.set((short) 4);
        intFieldInjector.set(5);
        longFieldInjector.set(6L);
        floatFieldInjector.set(7.1F);
        doubleFieldInjector.set(7.2D);
        Object portable = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record.readGenericRecord("null")).isNull();
        assertThat(record.readGenericRecord("object")).isNotNull();
        assertThat(record.readUTF("string")).isEqualTo("1");
        assertThat(record.readChar("character")).isEqualTo('2');
        assertThat(record.readBoolean("boolean")).isEqualTo(true);
        assertThat(record.readByte("byte")).isEqualTo((byte) 3);
        assertThat(record.readShort("short")).isEqualTo((short) 4);
        assertThat(record.readInt("int")).isEqualTo(5);
        assertThat(record.readLong("long")).isEqualTo(6L);
        assertThat(record.readFloat("float")).isEqualTo(7.1F);
        assertThat(record.readDouble("double")).isEqualTo(7.2D);
    }

    @SuppressWarnings({"unused", "checkstyle:LineLength"})
    private Object[] values() {
        ClassDefinition innerClassDefinition = new ClassDefinitionBuilder(4, 5, 6).build();

        return new Object[]{
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addPortableField("object", innerClassDefinition).build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addPortableField("object", innerClassDefinition).build(), new PortableGenericRecordBuilder(innerClassDefinition).build()},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addBooleanArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addBooleanArrayField("object").build(), new boolean[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addByteArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addByteArrayField("object").build(), new byte[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addShortArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addShortArrayField("object").build(), new short[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addCharArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addCharArrayField("object").build(), new char[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addIntArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addIntArrayField("object").build(), new int[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addLongArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addLongArrayField("object").build(), new long[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addFloatArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addFloatArrayField("object").build(), new float[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addDoubleArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addDoubleArrayField("object").build(), new double[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addUTFArrayField("object").build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addUTFArrayField("object").build(), new String[0]},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addPortableArrayField("object", innerClassDefinition).build(), null},
                new Object[]{new ClassDefinitionBuilder(1, 2, 3).addPortableArrayField("object", innerClassDefinition).build(), new GenericRecord[0]},
        };
    }

    @Test
    @Parameters(method = "values")
    public void when_typeIsObject_then_allValuesAreAllowed(
            ClassDefinition classDefinition,
            Object value
    ) throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("object", QueryDataType.OBJECT);

        target.init();
        injector.set(value);
        Object portable = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record.hasField("object")).isTrue();
    }

    @Test
    public void when_injectNonExistingPropertyValue_then_throws() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("field", QueryDataType.INT);

        target.init();
        assertThatThrownBy(() -> injector.set("1"))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Unable to inject a non-null value to \"field\"");
    }

    @Test
    public void when_injectNonExistingPropertyNullValue_then_succeeds() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("field", QueryDataType.INT);

        target.init();
        injector.set(null);
        Object portable = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record).isNotNull();
    }
}
