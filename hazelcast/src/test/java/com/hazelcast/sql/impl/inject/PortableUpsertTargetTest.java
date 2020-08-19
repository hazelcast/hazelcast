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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.PortableReader;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PortableUpsertTargetTest {

    @Test
    public void testInject() throws IOException {
        int factoryId = 1;
        int classId = 1;
        int versionId = 1;

        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        serializationService.getPortableContext()
                            .registerClassDefinition(
                                    new ClassDefinitionBuilder(factoryId, classId, versionId)
                                            .addBooleanField("boolean")
                                            .addByteField("byte")
                                            .addShortField("short")
                                            .addCharField("char")
                                            .addIntField("int")
                                            .addLongField("long")
                                            .addFloatField("float")
                                            .addDoubleField("double")
                                            .addUTFField("string")
                                            .build()
                            );

        UpsertTarget target = new PortableUpsertTarget(serializationService, factoryId, classId, versionId);

        UpsertInjector booleanInjector = target.createInjector("boolean");
        UpsertInjector byteInjector = target.createInjector("byte");
        UpsertInjector shortInjector = target.createInjector("short");
        UpsertInjector charInjector = target.createInjector("char");
        UpsertInjector intInjector = target.createInjector("int");
        UpsertInjector longInjector = target.createInjector("long");
        UpsertInjector floatInjector = target.createInjector("float");
        UpsertInjector doubleInjector = target.createInjector("double");
        UpsertInjector stringInjector = target.createInjector("string");

        target.init();
        booleanInjector.set(false);
        byteInjector.set((byte) 1);
        shortInjector.set((short) 1);
        charInjector.set('1');
        intInjector.set(1);
        longInjector.set(1L);
        floatInjector.set(1F);
        doubleInjector.set(1D);
        stringInjector.set("1");
        Object portable1 = target.conclude();
        PortableReader reader1 = serializationService.createPortableReader(serializationService.toData(portable1));
        assertFalse(reader1.readBoolean("boolean"));
        assertEquals((byte) 1, reader1.readByte("byte"));
        assertEquals((short) 1, reader1.readShort("short"));
        assertEquals('1', reader1.readChar("char"));
        assertEquals(1, reader1.readInt("int"));
        assertEquals(1L, reader1.readLong("long"));
        assertEquals(1F, reader1.readFloat("float"), 0.0);
        assertEquals(1D, reader1.readDouble("double"), 0.0);
        assertEquals("1", reader1.readUTF("string"));

        target.init();
        booleanInjector.set(true);
        byteInjector.set((byte) 2);
        shortInjector.set((short) 2);
        charInjector.set('2');
        intInjector.set(2);
        longInjector.set(2L);
        floatInjector.set(2F);
        doubleInjector.set(2D);
        stringInjector.set("2");
        Object portable2 = target.conclude();
        PortableReader reader2 = serializationService.createPortableReader(serializationService.toData(portable2));
        assertTrue(reader2.readBoolean("boolean"));
        assertEquals((byte) 2, reader2.readByte("byte"));
        assertEquals((short) 2, reader2.readShort("short"));
        assertEquals('2', reader2.readChar("char"));
        assertEquals(2, reader2.readInt("int"));
        assertEquals(2L, reader2.readLong("long"));
        assertEquals(2F, reader2.readFloat("float"), 0.0);
        assertEquals(2D, reader2.readDouble("double"), 0.0);
        assertEquals("2", reader2.readUTF("string"));
    }

    @Test
    public void testInjectNullIntoNonExistingField() throws IOException {
        int factoryId = 1;
        int classId = 1;
        int versionId = 1;

        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        serializationService.getPortableContext()
                            .registerClassDefinition(
                                    new ClassDefinitionBuilder(factoryId, classId, versionId)
                                            .build()
                            );

        UpsertTarget target = new PortableUpsertTarget(serializationService, factoryId, classId, versionId);

        UpsertInjector injector = target.createInjector("nonExistingField");

        target.init();
        injector.set(null);
        Object portable = target.conclude();

        PortableReader reader = serializationService.createPortableReader(serializationService.toData(portable));
        assertNotNull(reader);
    }

    @SuppressWarnings("unused")
    private static class Pojo {

        @SuppressWarnings("FieldCanBeLocal")
        private int field1;
        public String field2;

        public Pojo() {
        }

        public Pojo(int field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        public void setField1(int field1) {
            this.field1 = field1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Pojo pojo = (Pojo) o;
            return field1 == pojo.field1 &&
                    Objects.equals(field2, pojo.field2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }
}