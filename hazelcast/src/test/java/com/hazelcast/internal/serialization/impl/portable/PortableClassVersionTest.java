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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.internal.nio.IOUtil.readData;
import static com.hazelcast.internal.nio.IOUtil.writeData;
import static com.hazelcast.internal.serialization.impl.portable.PortableTest.createNamedPortableClassDefinition;
import static com.hazelcast.internal.serialization.impl.portable.PortableTest.createSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PortableClassVersionTest {

    private static final int FACTORY_ID = TestSerializationConstants.PORTABLE_FACTORY_ID;

    @Test
    public void testDifferentClassVersions() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }
                }).build();

        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersions(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersions() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().setPortableVersion(1)
                                                                                            .addPortableFactory(FACTORY_ID, new PortableFactory() {
                                                                                                public Portable create(int classId) {
                                                                                                    return new NamedPortable();
                                                                                                }
                                                                                            }).build();

        SerializationService serializationService2 = new DefaultSerializationServiceBuilder().setPortableVersion(2)
                                                                                             .addPortableFactory(FACTORY_ID, new PortableFactory() {
                                                                                                 public Portable create(int classId) {
                                                                                                     return new NamedPortableV2();
                                                                                                 }
                                                                                             }).build();

        testDifferentClassVersions(serializationService, serializationService2);
    }

    // used in EE, so needs to be package private
    static void testDifferentClassVersions(SerializationService serializationService,
                                           SerializationService serializationService2) {
        NamedPortable portableV1 = new NamedPortable("named-portable", 123);
        Data dataV1 = serializationService.toData(portableV1);

        NamedPortableV2 portableV2 = new NamedPortableV2("named-portable", 123, 500);
        Data dataV2 = serializationService2.toData(portableV2);

        NamedPortable v1FromV2 = serializationService.toObject(dataV2);
        assertEquals(portableV2.name, v1FromV2.name);
        assertEquals(portableV2.myint, v1FromV2.myint);

        NamedPortableV2 v2FromV1 = serializationService2.toObject(dataV1);
        assertEquals(portableV1.name, v2FromV1.name);
        assertEquals(portableV1.myint, v2FromV1.myint);
        assertNull(v2FromV1.v);
    }

    @Test
    public void testDifferentClassVersionsUsingDataWriteAndRead() throws Exception {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }
                }).build();

        InternalSerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        testDifferentClassVersionsUsingDataWriteAndRead(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersionsUsingDataWriteAndRead() throws Exception {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().setPortableVersion(1)
                                                                                                    .addPortableFactory(FACTORY_ID, new PortableFactory() {
                                                                                                        public Portable create(int classId) {
                                                                                                            return new NamedPortable();
                                                                                                        }
                                                                                                    }).build();

        InternalSerializationService serializationService2 = new DefaultSerializationServiceBuilder().setPortableVersion(2)
                                                                                                     .addPortableFactory(FACTORY_ID, new PortableFactory() {
                                                                                                         public Portable create(int classId) {
                                                                                                             return new NamedPortableV2();
                                                                                                         }
                                                                                                     }).build();

        testDifferentClassVersionsUsingDataWriteAndRead(serializationService, serializationService2);
    }

    // used in EE, so needs to be package private
    static void testDifferentClassVersionsUsingDataWriteAndRead(InternalSerializationService serializationService,
                                                                InternalSerializationService serializationService2)
            throws Exception {
        NamedPortable portableV1 = new NamedPortable("portable-v1", 111);
        Data dataV1 = serializationService.toData(portableV1);

        // emulate socket write by writing data to stream
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024);
        writeData(out, dataV1);
        byte[] bytes = out.toByteArray();
        // emulate socket read by reading data from stream
        BufferObjectDataInput in = serializationService2.createObjectDataInput(bytes);
        dataV1 = readData(in);

        // serialize new portable version
        NamedPortableV2 portableV2 = new NamedPortableV2("portable-v2", 123, 500);
        Data dataV2 = serializationService2.toData(portableV2);

        NamedPortable v1FromV2 = serializationService.toObject(dataV2);
        assertEquals(portableV2.name, v1FromV2.name);
        assertEquals(portableV2.myint, v1FromV2.myint);

        NamedPortableV2 v2FromV1 = serializationService2.toObject(dataV1);
        assertEquals(portableV1.name, v2FromV1.name);
        assertEquals(portableV1.myint, v2FromV1.myint);
        assertNull(v2FromV1.v);
    }

    @Test
    public void testPreDefinedDifferentVersionsWithInnerPortable() {
        InternalSerializationService serializationService = createSerializationService(1);
        serializationService.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(1));

        InternalSerializationService serializationService2 = createSerializationService(2);
        serializationService2.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(2));

        NamedPortable[] nn = new NamedPortable[1];
        nn[0] = new NamedPortable("name", 123);
        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn,
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now()},
                new OffsetDateTime[]{OffsetDateTime.now(), OffsetDateTime.now()});

        MainPortable mainWithInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now());

        testPreDefinedDifferentVersions(serializationService, serializationService2, mainWithInner);
    }

    @Test
    public void testPreDefinedDifferentVersionsWithNullInnerPortable_and_nullObjects() {
        InternalSerializationService serializationService = createSerializationService(1);
        serializationService.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(1));

        InternalSerializationService serializationService2 = createSerializationService(2);
        serializationService2.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(2));

        MainPortable mainWithNullInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", null, null,
                null, null, null, null);

        testPreDefinedDifferentVersions(serializationService, serializationService2, mainWithNullInner);
    }

    private static void testPreDefinedDifferentVersions(SerializationService serializationService,
                                                        SerializationService serializationService2,
                                                        MainPortable mainPortable) {
        Data data = serializationService.toData(mainPortable);

        assertEquals(mainPortable, serializationService2.toObject(data));
    }

    // used in EE, so needs to be package private
    static ClassDefinition createInnerPortableClassDefinition(int portableVersion) {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(FACTORY_ID, TestSerializationConstants.INNER_PORTABLE,
                portableVersion);
        builder.addByteArrayField("b");
        builder.addCharArrayField("c");
        builder.addShortArrayField("s");
        builder.addIntArrayField("i");
        builder.addLongArrayField("l");
        builder.addFloatArrayField("f");
        builder.addDoubleArrayField("d");
        builder.addPortableArrayField("nn", createNamedPortableClassDefinition(portableVersion));
        builder.addDecimalArrayField("bigDecimals");
        builder.addTimeArrayField("localTimes");
        builder.addDateArrayField("localDates");
        builder.addTimestampArrayField("localDateTimes");
        builder.addTimestampWithTimezoneArrayField("offsetDateTimes");
        return builder.build();
    }
}
