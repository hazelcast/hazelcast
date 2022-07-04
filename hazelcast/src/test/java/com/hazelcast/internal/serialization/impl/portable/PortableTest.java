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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.CustomSerializationTest;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.SampleIdentifiedDataSerializable;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PortableTest {

    public static final int PORTABLE_FACTORY_ID = TestSerializationConstants.PORTABLE_FACTORY_ID;
    static final int IDENTIFIED_FACTORY_ID = TestSerializationConstants.DATA_SERIALIZABLE_FACTORY_ID;

    @Test
    public void testBasics() {
        testBasics(ByteOrder.BIG_ENDIAN, false);
    }

    @Test
    public void testBasicsLittleEndian() {
        testBasics(ByteOrder.LITTLE_ENDIAN, false);
    }

    @Test
    public void testBasicsNativeOrder() {
        testBasics(ByteOrder.nativeOrder(), false);
    }

    @Test
    public void testBasicsNativeOrderUsingUnsafe() {
        testBasics(ByteOrder.nativeOrder(), true);
    }

    private void testBasics(ByteOrder order, boolean allowUnsafe) {
        final SerializationService serializationService = createSerializationService(1, order, allowUnsafe);
        final SerializationService serializationService2 = createSerializationService(2, order, allowUnsafe);
        Data data;

        NamedPortable[] nn = new NamedPortable[5];
        for (int i = 0; i < nn.length; i++) {
            nn[i] = new NamedPortable("named-portable-" + i, i);
        }

        NamedPortable np = nn[0];
        data = serializationService.toData(np);
        assertEquals(np, serializationService.toObject(data));
        assertEquals(np, serializationService2.toObject(data));

        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn,
                new BigDecimal[]{new BigDecimal(new BigInteger("12345"), 10), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now()},
                new OffsetDateTime[]{OffsetDateTime.now(), OffsetDateTime.now()});

        data = serializationService.toData(inner);
        assertEquals(inner, serializationService.toObject(data));
        assertEquals(inner, serializationService2.toObject(data));

        MainPortable main = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now());

        data = serializationService.toData(main);
        assertEquals(main, serializationService.toObject(data));
        assertEquals(main, serializationService2.toObject(data));
    }

    static InternalSerializationService createSerializationService(int version) {
        return createSerializationService(version, ByteOrder.BIG_ENDIAN, false);
    }

    static InternalSerializationService createSerializationService(int version, ByteOrder order, boolean allowUnsafe) {
        return new DefaultSerializationServiceBuilder()
                .setUseNativeByteOrder(false).setAllowUnsafe(allowUnsafe).setByteOrder(order).setPortableVersion(version)
                .addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory())
                .addDataSerializableFactory(IDENTIFIED_FACTORY_ID, new TestDataSerializableFactory()).build();
    }

    static ClassDefinition createNamedPortableClassDefinition(int portableVersion) {
        ClassDefinitionBuilder builder
                = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE, portableVersion);
        builder.addStringField("name");
        builder.addIntField("myint");
        return builder.build();
    }

    @Test
    public void testRawData() {
        int portableVersion = 1;
        final InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId(), portableVersion);
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(portableVersion));
        serializationService.getPortableContext().registerClassDefinition(builder.build());

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testRawDataWithoutRegistering() {
        final SerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRawDataInvalidWrite() {
        int portableVersion = 1;
        final InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new InvalidRawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId(), portableVersion);
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(portableVersion));
        serializationService.getPortableContext().registerClassDefinition(builder.build());

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRawDataInvalidRead() {
        int portableVersion = 1;
        final InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new InvalidRawDataPortable2(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId(), portableVersion);
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(portableVersion));
        serializationService.getPortableContext().registerClassDefinition(builder.build());

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testClassDefinitionConfigWithErrors() throws Exception {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory());
        serializationConfig.setPortableVersion(1);
        serializationConfig.addClassDefinition(
                new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, TestSerializationConstants.RAW_DATA_PORTABLE, 1)
                        .addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(1)).build());

        try {
            new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
            fail("Should throw HazelcastSerializationException!");
        } catch (HazelcastSerializationException ignored) {
        }

        new DefaultSerializationServiceBuilder().setConfig(serializationConfig).setCheckClassDefErrors(false).build();

        // -- OR --

        serializationConfig.setCheckClassDefErrors(false);
        new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
    }

    @Test
    public void testClassDefinitionConfig() throws Exception {
        int portableVersion = 1;
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory());
        serializationConfig.setPortableVersion(portableVersion);
        serializationConfig
                .addClassDefinition(
                        new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, TestSerializationConstants.RAW_DATA_PORTABLE, portableVersion)
                                .addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(portableVersion)).build())
                .addClassDefinition(
                        new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE, portableVersion)
                                .addStringField("name").addIntField("myint").build());

        SerializationService serializationService
                = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testPortableNestedInOthers() {
        SerializationService serializationService = createSerializationService(1);
        Object o1 = new ComplexDataSerializable(new NamedPortable("test-portable", 137),
                new ByteArrayDataSerializable("test-data-serializable".getBytes()),
                new ByteArrayDataSerializable("test-data-serializable-2".getBytes()));

        Data data = serializationService.toData(o1);

        SerializationService serializationService2 = createSerializationService(2);

        Object o2 = serializationService2.toObject(data);
        assertEquals(o1, o2);
    }

    //https://github.com/hazelcast/hazelcast/issues/1096
    @Test
    public void test_1096_ByteArrayContentSame() {
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory()).build();

        assertRepeatedSerialisationGivesSameByteArrays(ss, new NamedPortable("issue-1096", 1096));

        assertRepeatedSerialisationGivesSameByteArrays(ss, new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321},
                new NamedPortable[]{new NamedPortable("issue-1096", 1096)},
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now()},
                new OffsetDateTime[]{OffsetDateTime.now()}));

        assertRepeatedSerialisationGivesSameByteArrays(ss,
                new RawDataPortable(1096L, "issue-1096".toCharArray(), new NamedPortable("issue-1096", 1096), 1096,
                        "issue-1096", new ByteArrayDataSerializable(new byte[1])));
    }

    private static void assertRepeatedSerialisationGivesSameByteArrays(SerializationService ss, Portable p) {
        Data data1 = ss.toData(p);
        for (int k = 0; k < 100; k++) {
            Data data2 = ss.toData(p);
            assertEquals(data1, data2);
        }
    }

    //https://github.com/hazelcast/hazelcast/issues/2172
    @Test
    public void test_issue2172_WritePortableArray() {
        final SerializationService ss = new DefaultSerializationServiceBuilder().setInitialOutputBufferSize(16).build();
        final TestObject2[] testObject2s = new TestObject2[100];
        for (int i = 0; i < testObject2s.length; i++) {
            testObject2s[i] = new TestObject2();
        }

        final TestObject1 testObject1 = new TestObject1(testObject2s);
        ss.toData(testObject1);
    }

    @Test
    public void testClassDefinitionLookupBigEndianHeapData() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setByteOrder(ByteOrder.BIG_ENDIAN)
                .build();

        testClassDefinitionLookup(ss);
    }

    @Test
    public void testClassDefinitionLookupLittleEndianHeapData() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setByteOrder(ByteOrder.LITTLE_ENDIAN)
                .build();

        testClassDefinitionLookup(ss);
    }

    @Test
    public void testClassDefinitionLookupNativeOrderHeapData() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setUseNativeByteOrder(true)
                .build();

        testClassDefinitionLookup(ss);
    }

    static void testClassDefinitionLookup(InternalSerializationService ss) throws IOException {
        NamedPortableV2 p = new NamedPortableV2("test-portable", 123456789, 500);
        Data data = ss.toData(p);

        PortableContext portableContext = ss.getPortableContext();
        ClassDefinition cd = portableContext.lookupClassDefinition(data);
        assertNotNull(cd);

        assertEquals(p.getFactoryId(), cd.getFactoryId());
        assertEquals(p.getClassId(), cd.getClassId());
        assertEquals(p.getClassVersion(), cd.getVersion());
    }

    @Test
    public void testSerializationService_createPortableReader() throws IOException {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        long timestamp1 = System.nanoTime();
        ChildPortableObject child = new ChildPortableObject(timestamp1);
        long timestamp2 = System.currentTimeMillis();
        ParentPortableObject parent = new ParentPortableObject(timestamp2, child);
        long timestamp3 = timestamp1 + timestamp2;
        GrandParentPortableObject grandParent = new GrandParentPortableObject(timestamp3, parent);

        Data data = serializationService.toData(grandParent);
        GenericRecordQueryReader reader = new GenericRecordQueryReader(serializationService.readAsInternalGenericRecord(data));

        assertEquals(grandParent.timestamp, reader.read("timestamp"));
        assertEquals(parent.timestamp, reader.read("child.timestamp"));
        assertEquals(child.timestamp, reader.read("child.child.timestamp"));
    }

    @Test
    public void testClassDefinition_getNestedField() throws IOException {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        PortableContext portableContext = serializationService.getPortableContext();

        ChildPortableObject child = new ChildPortableObject(System.nanoTime());
        ParentPortableObject parent = new ParentPortableObject(System.currentTimeMillis(), child);
        GrandParentPortableObject grandParent = new GrandParentPortableObject(System.nanoTime(), parent);

        Data data = serializationService.toData(grandParent);
        ClassDefinition classDefinition = portableContext.lookupClassDefinition(data);

        FieldDefinition fd = portableContext.getFieldDefinition(classDefinition, "child");
        assertNotNull(fd);
        assertEquals(FieldType.PORTABLE, fd.getType());

        fd = portableContext.getFieldDefinition(classDefinition, "child.child");
        assertNotNull(fd);
        assertEquals(FieldType.PORTABLE, fd.getType());

        fd = portableContext.getFieldDefinition(classDefinition, "child.child.timestamp");
        assertNotNull(fd);
        assertEquals(FieldType.LONG, fd.getType());

    }

    @Test
    public void testWriteRead_withNullPortableArray() {
        int portableVersion = 1;
        ClassDefinitionBuilder builder0 = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, 1, portableVersion);
        ClassDefinitionBuilder builder1 = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, 2, portableVersion);
        builder0.addPortableArrayField("list", builder1.build());

        SerializationService ss = new DefaultSerializationServiceBuilder()
                .setPortableVersion(portableVersion)
                .addClassDefinition(builder0.build())
                .addClassDefinition(builder1.build())
                .build();

        Data data = ss.toData(new TestObject1());

        SerializationService ss2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(1, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        switch (classId) {
                            case 1:
                                return new TestObject1();
                            case 2:
                                return new TestObject2();
                            default:
                                return null;
                        }
                    }
                })
                .build();

        Object object = ss2.toObject(data);
        assertNotNull(object);
        assertTrue(object instanceof TestObject1);
    }

    public static class GrandParentPortableObject implements Portable {

        long timestamp;
        ParentPortableObject child;

        public GrandParentPortableObject(long timestamp) {
            this.timestamp = timestamp;
            child = new ParentPortableObject(timestamp);
        }

        public GrandParentPortableObject(long timestamp, ParentPortableObject child) {
            this.timestamp = timestamp;
            this.child = child;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            timestamp = reader.readLong("timestamp");
            child = reader.readPortable("child");
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("timestamp", timestamp);
            writer.writePortable("child", child);
        }
    }

    public static class ParentPortableObject implements Portable {

        long timestamp;
        ChildPortableObject child;

        public ParentPortableObject(long timestamp) {
            this.timestamp = timestamp;
            child = new ChildPortableObject(timestamp);
        }

        public ParentPortableObject(long timestamp, ChildPortableObject child) {
            this.timestamp = timestamp;
            this.child = child;
        }

        @Override
        public int getFactoryId() {
            return 2;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            timestamp = reader.readLong("timestamp");
            child = reader.readPortable("child");
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("timestamp", timestamp);
            writer.writePortable("child", child);
        }
    }

    public static class ChildPortableObject implements Portable {

        long timestamp;

        public ChildPortableObject(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public int getFactoryId() {
            return 3;
        }

        @Override
        public int getClassId() {
            return 3;
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            timestamp = reader.readLong("timestamp");
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("timestamp", timestamp);
        }
    }

    static class TestObject1 implements Portable {

        private Portable[] portables;

        TestObject1() {
        }

        TestObject1(Portable[] p) {
            portables = p;
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortableArray("list", portables);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            portables = reader.readPortableArray("list");
        }
    }

    static class TestObject2 implements Portable {

        private String shortString;

        TestObject2() {
            shortString = "Hello World";
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("shortString", shortString);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestPortableFactory implements PortableFactory {

        @Override
        public Portable create(int classId) {
            switch (classId) {
                case TestSerializationConstants.MAIN_PORTABLE:
                    return new MainPortable();
                case TestSerializationConstants.INNER_PORTABLE:
                    return new InnerPortable();
                case TestSerializationConstants.NAMED_PORTABLE:
                    return new NamedPortable();
                case TestSerializationConstants.RAW_DATA_PORTABLE:
                    return new RawDataPortable();
                case TestSerializationConstants.INVALID_RAW_DATA_PORTABLE:
                    return new InvalidRawDataPortable();
                case TestSerializationConstants.INVALID_RAW_DATA_PORTABLE_2:
                    return new InvalidRawDataPortable2();
                case TestSerializationConstants.OBJECT_CARRYING_PORTABLE:
                    return new ObjectCarryingPortable();
                case TestSerializationConstants.ALL_FIELD_OBJECT_PORTABLE:
                    return new SerializationV1Portable();
                default:
                    return null;
            }
        }
    }

    public static class TestDataSerializableFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case TestSerializationConstants.SAMPLE_IDENTIFIED_DATA_SERIALIZABLE:
                    return new SampleIdentifiedDataSerializable();
                default:
                    return null;
            }
        }
    }

    @Test
    public void testWriteObject_withPortable() {
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(PORTABLE_FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                })
                .build();

        SerializationService ss2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(PORTABLE_FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }
                })
                .setPortableVersion(5)
                .build();

        Object o1 = new ComplexDataSerializable(new NamedPortableV2("test", 123, 500),
                new ByteArrayDataSerializable(new byte[3]), null);
        Data data = ss.toData(o1);

        Object o2 = ss2.toObject(data);
        assertEquals(o1, o2);
    }

    @Test
    public void testWriteData_withPortable() {
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(PORTABLE_FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                })
                .build();

        SerializationService ss2 = new DefaultSerializationServiceBuilder()
                .addPortableFactory(PORTABLE_FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }
                })
                .setPortableVersion(5)
                .build();

        Portable p1 = new NamedPortableV2("test", 456, 500);
        Object o1 = new DataDataSerializable(ss.toData(p1));

        Data data = ss.toData(o1);

        DataDataSerializable o2 = ss2.toObject(data);
        assertEquals(o1, o2);

        Portable p2 = ss2.toObject(o2.data);
        assertEquals(p1, p2);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testGenericPortable_whenMultipleTypesAreUsed() {
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(1, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        switch (classId) {
                            case ParentGenericPortable.CLASS_ID:
                                return new ParentGenericPortable();
                            case ChildGenericPortable1.CLASS_ID:
                                return new ChildGenericPortable1();
                            case ChildGenericPortable2.CLASS_ID:
                                return new ChildGenericPortable2();
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                }).build();


        ss.toData(new ParentGenericPortable<ChildGenericPortable1>(new ChildGenericPortable1("aaa", "bbb")));

        Data data = ss.toData(new ParentGenericPortable<ChildGenericPortable2>(new ChildGenericPortable2("ccc")));
        ss.toObject(data);

    }

    @Test
    public void testWriteObjectWithPortable() {
        SerializationService serializationService = createSerializationService(1);

        NamedPortable namedPortable = new NamedPortable("name", 2);
        ObjectCarryingPortable objectCarryingPortable1 = new ObjectCarryingPortable(namedPortable);
        Data data = serializationService.toData(objectCarryingPortable1);
        ObjectCarryingPortable objectCarryingPortable2 = serializationService.toObject(data);
        assertEquals(objectCarryingPortable1, objectCarryingPortable2);
    }

    @Test
    public void testWriteObjectWithIdentifiedDataSerializable() {
        SerializationService serializationService = createSerializationService(1);

        SampleIdentifiedDataSerializable namedPortable = new SampleIdentifiedDataSerializable('c', 2);
        ObjectCarryingPortable objectCarryingPortable1 = new ObjectCarryingPortable(namedPortable);
        Data data = serializationService.toData(objectCarryingPortable1);
        ObjectCarryingPortable objectCarryingPortable2 = serializationService.toObject(data);
        assertEquals(objectCarryingPortable1, objectCarryingPortable2);
    }

    @Test
    public void testWriteObjectWithCustomSerializable() {
        SerializationConfig config = new SerializationConfig();
        SerializerConfig sc = new SerializerConfig()
                .setImplementation(new CustomSerializationTest.FooXmlSerializer())
                .setTypeClass(CustomSerializationTest.Foo.class);
        config.addSerializerConfig(sc);
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setPortableVersion(1)
                .addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory()).setConfig(config).build();

        CustomSerializationTest.Foo foo = new CustomSerializationTest.Foo("f");

        ObjectCarryingPortable objectCarryingPortable1 = new ObjectCarryingPortable(foo);
        Data data = serializationService.toData(objectCarryingPortable1);
        ObjectCarryingPortable objectCarryingPortable2 = serializationService.toObject(data);
        assertEquals(objectCarryingPortable1, objectCarryingPortable2);


    }

    static class ParentGenericPortable<T extends Portable> implements Portable {
        static final int CLASS_ID = 1;

        T child;

        ParentGenericPortable() {
        }

        ParentGenericPortable(T child) {
            this.child = child;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortable("c", child);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            child = reader.readPortable("c");
        }
    }

    static class ChildGenericPortable1 implements Portable {
        static final int CLASS_ID = 2;

        String s1;
        String s2;

        ChildGenericPortable1() {
        }

        ChildGenericPortable1(String s1, String s2) {
            this.s1 = s1;
            this.s2 = s2;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("s1", s1);
            writer.writeString("s2", s2);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            s1 = reader.readString("s1");
            s2 = reader.readString("s2");
        }
    }

    static class ChildGenericPortable2 implements Portable {
        static final int CLASS_ID = 3;

        String s;

        ChildGenericPortable2() {
        }

        ChildGenericPortable2(String s1) {
            this.s = s1;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("s", s);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            s = reader.readString("s");
        }
    }

}
