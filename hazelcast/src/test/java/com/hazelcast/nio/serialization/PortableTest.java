/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author mdogan 1/4/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PortableTest {

    static final int FACTORY_ID = 1;

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
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        data = serializationService.toData(inner);
        assertEquals(inner, serializationService.toObject(data));
        assertEquals(inner, serializationService2.toObject(data));

        MainPortable main = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        data = serializationService.toData(main);
        assertEquals(main, serializationService.toObject(data));
        assertEquals(main, serializationService2.toObject(data));
    }

    private SerializationService createSerializationService(int version) {
        return createSerializationService(version, ByteOrder.BIG_ENDIAN, false);
    }

    private SerializationService createSerializationService(int version, ByteOrder order, boolean allowUnsafe) {
        return new SerializationServiceBuilder()
                .setUseNativeByteOrder(false).setAllowUnsafe(allowUnsafe).setByteOrder(order).setVersion(version)
                .addPortableFactory(FACTORY_ID, new TestPortableFactory()).build();
    }

    @Test
    public void testDifferentVersions() {
        final SerializationService serializationService = new SerializationServiceBuilder().setVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        final SerializationService serializationService2 = new SerializationServiceBuilder().setVersion(2)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        NamedPortable p1 = new NamedPortable("portable-v1", 111);
        Data data = serializationService.toData(p1);

        NamedPortableV2 p2 = new NamedPortableV2("portable-v2", 123);
        Data data2 = serializationService2.toData(p2);

        serializationService2.toObject(data);
        serializationService.toObject(data2);
    }

    @Test
    public void testDifferentVersionsUsingDataWriteAndRead() throws IOException {
        final SerializationService serializationService = new SerializationServiceBuilder().setVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        final SerializationService serializationService2 = new SerializationServiceBuilder().setVersion(2)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        NamedPortable p1 = new NamedPortable("portable-v1", 111);
        Data data = serializationService.toData(p1);

        // emulate socket write by writing data to stream
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024);
        data.writeData(out);
        byte[] bytes = out.toByteArray();

        // emulate socket read by reading data from stream
        BufferObjectDataInput in = serializationService2.createObjectDataInput(bytes);
        data = new Data();
        data.readData(in);

        // register class def and read data
        Object object1 = serializationService2.toObject(data);

        // serialize new portable version
        NamedPortableV2 p2 = new NamedPortableV2("portable-v2", 123);
        Data data2 = serializationService2.toData(p2);

        // de-serialize back using old version
        Object object2 = serializationService.toObject(data2);

        assertTrue(object1 instanceof NamedPortableV2);
        assertTrue(object2 instanceof NamedPortable);
    }

    @Test
    public void testPreDefinedDifferentVersions() {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(FACTORY_ID, InnerPortable.CLASS_ID);
        builder.addByteArrayField("b");
        builder.addCharArrayField("c");
        builder.addShortArrayField("s");
        builder.addIntArrayField("i");
        builder.addLongArrayField("l");
        builder.addFloatArrayField("f");
        builder.addDoubleArrayField("d");
        ClassDefinition cd = createNamedPortableClassDefinition();
        builder.addPortableArrayField("nn", cd);

        final SerializationService serializationService = createSerializationService(1);
        serializationService.getSerializationContext().registerClassDefinition(builder.build());

        final SerializationService serializationService2 = createSerializationService(2);
        serializationService2.getSerializationContext().registerClassDefinition(builder.build());

        final MainPortable mainWithNullInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", null);

        final Data data = serializationService.toData(mainWithNullInner);
        assertEquals(mainWithNullInner, serializationService2.toObject(data));

        NamedPortable[] nn = new NamedPortable[1];
        nn[0] = new NamedPortable("name", 123);
        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        final MainPortable mainWithInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        final Data data2 = serializationService.toData(mainWithInner);
        assertEquals(mainWithInner, serializationService2.toObject(data2));
    }

    private ClassDefinition createNamedPortableClassDefinition() {
        ClassDefinitionBuilder builder2 = new ClassDefinitionBuilder(FACTORY_ID, NamedPortable.CLASS_ID);
        builder2.addUTFField("name");
        builder2.addIntField("myint");
        return builder2.build();
    }

    @Test
    public void testRawData() {
        final SerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new SimpleDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId());
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition());
        serializationService.getSerializationContext().registerClassDefinition(builder.build());

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testRawDataWithoutRegistering() {
        final SerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new SimpleDataSerializable("test bytes".getBytes()));

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRawDataInvalidWrite() {
        final SerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new InvalidRawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new SimpleDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId());
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition());
        serializationService.getSerializationContext().registerClassDefinition(builder.build());

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRawDataInvalidRead() {
        final SerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new InvalidRawDataPortable2(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new SimpleDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId());
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition());
        serializationService.getSerializationContext().registerClassDefinition(builder.build());

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testClassDefinitionConfigWithErrors() throws Exception {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(FACTORY_ID, new TestPortableFactory());
        serializationConfig.setPortableVersion(1);
        serializationConfig.addClassDefinition(
                new ClassDefinitionBuilder(FACTORY_ID, RawDataPortable.CLASS_ID)
                        .addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition()).build());

        try {
            new SerializationServiceBuilder().setConfig(serializationConfig).build();
            fail("Should throw HazelcastSerializationException!");
        } catch (HazelcastSerializationException e) {
        }

        new SerializationServiceBuilder().setConfig(serializationConfig).setCheckClassDefErrors(false).build();

        // -- OR --

        serializationConfig.setCheckClassDefErrors(false);
        new SerializationServiceBuilder().setConfig(serializationConfig).build();
    }

    @Test
    public void testClassDefinitionConfig() throws Exception {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(FACTORY_ID, new TestPortableFactory());
        serializationConfig.setPortableVersion(1);
        serializationConfig
                .addClassDefinition(
                        new ClassDefinitionBuilder(FACTORY_ID, RawDataPortable.CLASS_ID)
                                .addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition()).build())
                .addClassDefinition(
                        new ClassDefinitionBuilder(FACTORY_ID, NamedPortable.CLASS_ID)
                                .addUTFField("name").addIntField("myint").build()
                );

        SerializationService serializationService = new SerializationServiceBuilder().setConfig(serializationConfig).build();
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new SimpleDataSerializable("test bytes".getBytes()));

        final Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testPortableNestedInOthers() {
        SerializationService serializationService = createSerializationService(1);
        Object o1 = new ComplexDataSerializable(new NamedPortable("test-portable", 137),
                new SimpleDataSerializable("test-data-serializable".getBytes()),
                new SimpleDataSerializable("test-data-serializable-2".getBytes()));

        Data data = serializationService.toData(o1);
        SerializationService serializationService2 = createSerializationService(2);
        Object o2 = serializationService2.toObject(data);
        assertEquals(o1, o2);
    }

    //https://github.com/hazelcast/hazelcast/issues/1096
    @Test
    public void test_1096_ByteArrayContentSame(){
        SerializationService ss = new SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new TestPortableFactory()).build();

        assertRepeatedSerialisationGivesSameByteArrays(ss, new NamedPortable("issue-1096", 1096));

        assertRepeatedSerialisationGivesSameByteArrays(ss, new InnerPortable(new byte[3], new char[5], new short[2],
                new int[10], new long[7], new float[9], new double[1], new NamedPortable[]{new NamedPortable("issue-1096", 1096)}));

        assertRepeatedSerialisationGivesSameByteArrays(ss, new RawDataPortable(1096L, "issue-1096".toCharArray(),
                new NamedPortable("issue-1096", 1096), 1096, "issue-1096", new SimpleDataSerializable(new byte[1])));
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
        final SerializationService ss = new SerializationServiceBuilder().setInitialOutputBufferSize(16).build();

        final TestObject2[] testObject2s = new TestObject2[100];
        for (int i = 0; i < testObject2s.length; i++) {
            testObject2s[i] = new TestObject2();
        }
        final TestObject1 testObject1 = new TestObject1(testObject2s);

        ss.toData(testObject1);

    }

    class TestObject1 implements Portable {

        private Portable[] portables;

        public TestObject1() {
        }

        public TestObject1(Portable[] p) {
            portables = p;
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
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
            throw new UnsupportedOperationException();
        }
    }

    class TestObject2 implements Portable {

        private String shortString;

        public TestObject2() {
            shortString = "Hello World";
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("shortString", shortString);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestPortableFactory implements PortableFactory {

        public Portable create(int classId) {
            switch (classId) {
                case MainPortable.CLASS_ID:
                    return new MainPortable();
                case InnerPortable.CLASS_ID:
                    return new InnerPortable();
                case NamedPortable.CLASS_ID:
                    return new NamedPortable();
                case RawDataPortable.CLASS_ID:
                    return new RawDataPortable();
                case InvalidRawDataPortable.CLASS_ID:
                    return new InvalidRawDataPortable();
                case InvalidRawDataPortable2.CLASS_ID:
                    return new InvalidRawDataPortable2();
            }
            return null;
        }
    }

    public static class MainPortable implements Portable {

        static final int CLASS_ID = 1;

        byte b;
        boolean bool;
        char c;
        short s;
        int i;
        long l;
        float f;
        double d;
        String str;
        InnerPortable p;

        private MainPortable() {
        }

        private MainPortable(byte b, boolean bool, char c, short s, int i, long l, float f,
                             double d, String str, InnerPortable p) {
            this.b = b;
            this.bool = bool;
            this.c = c;
            this.s = s;
            this.i = i;
            this.l = l;
            this.f = f;
            this.d = d;
            this.str = str;
            this.p = p;
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByte("b", b);
            writer.writeBoolean("bool", bool);
            writer.writeChar("c", c);
            writer.writeShort("s", s);
            writer.writeInt("i", i);
            writer.writeLong("l", l);
            writer.writeFloat("f", f);
            writer.writeDouble("d", d);
            writer.writeUTF("str", str);
            if (p != null) {
                writer.writePortable("p", p);
            } else {
                writer.writeNullPortable("p", FACTORY_ID, InnerPortable.CLASS_ID);
            }
        }

        public void readPortable(PortableReader reader) throws IOException {
            b = reader.readByte("b");
            bool = reader.readBoolean("bool");
            c = reader.readChar("c");
            s = reader.readShort("s");
            i = reader.readInt("i");
            l = reader.readLong("l");
            f = reader.readFloat("f");
            d = reader.readDouble("d");
            str = reader.readUTF("str");
            p = reader.readPortable("p");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MainPortable that = (MainPortable) o;

            if (b != that.b) return false;
            if (bool != that.bool) return false;
            if (c != that.c) return false;
            if (Double.compare(that.d, d) != 0) return false;
            if (Float.compare(that.f, f) != 0) return false;
            if (i != that.i) return false;
            if (l != that.l) return false;
            if (s != that.s) return false;
            if (p != null ? !p.equals(that.p) : that.p != null) return false;
            if (str != null ? !str.equals(that.str) : that.str != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = (int) b;
            result = 31 * result + (bool ? 1 : 0);
            result = 31 * result + (int) c;
            result = 31 * result + (int) s;
            result = 31 * result + i;
            result = 31 * result + (int) (l ^ (l >>> 32));
            result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
            temp = d != +0.0d ? Double.doubleToLongBits(d) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + (str != null ? str.hashCode() : 0);
            result = 31 * result + (p != null ? p.hashCode() : 0);
            return result;
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    public static class InnerPortable implements Portable {

        static final int CLASS_ID = 2;

        byte[] bb;
        char[] cc;
        short[] ss;
        int[] ii;
        long[] ll;
        float[] ff;
        double[] dd;
        NamedPortable[] nn;

        private InnerPortable() {
        }

        private InnerPortable(byte[] bb, char[] cc, short[] ss, int[] ii, long[] ll,
                              float[] ff, double[] dd, NamedPortable[] nn) {
            this.bb = bb;
            this.cc = cc;
            this.ss = ss;
            this.ii = ii;
            this.ll = ll;
            this.ff = ff;
            this.dd = dd;
            this.nn = nn;
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByteArray("b", bb);
            writer.writeCharArray("c", cc);
            writer.writeShortArray("s", ss);
            writer.writeIntArray("i", ii);
            writer.writeLongArray("l", ll);
            writer.writeFloatArray("f", ff);
            writer.writeDoubleArray("d", dd);
            writer.writePortableArray("nn", nn);
        }

        public void readPortable(PortableReader reader) throws IOException {
            bb = reader.readByteArray("b");
            cc = reader.readCharArray("c");
            ss = reader.readShortArray("s");
            ii = reader.readIntArray("i");
            ll = reader.readLongArray("l");
            ff = reader.readFloatArray("f");
            dd = reader.readDoubleArray("d");
            final Portable[] pp = reader.readPortableArray("nn");
            nn = new NamedPortable[pp.length];
            System.arraycopy(pp, 0, nn, 0, nn.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InnerPortable that = (InnerPortable) o;

            if (!Arrays.equals(bb, that.bb)) return false;
            if (!Arrays.equals(cc, that.cc)) return false;
            if (!Arrays.equals(dd, that.dd)) return false;
            if (!Arrays.equals(ff, that.ff)) return false;
            if (!Arrays.equals(ii, that.ii)) return false;
            if (!Arrays.equals(ll, that.ll)) return false;
            if (!Arrays.equals(nn, that.nn)) return false;
            if (!Arrays.equals(ss, that.ss)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = bb != null ? Arrays.hashCode(bb) : 0;
            result = 31 * result + (cc != null ? Arrays.hashCode(cc) : 0);
            result = 31 * result + (ss != null ? Arrays.hashCode(ss) : 0);
            result = 31 * result + (ii != null ? Arrays.hashCode(ii) : 0);
            result = 31 * result + (ll != null ? Arrays.hashCode(ll) : 0);
            result = 31 * result + (ff != null ? Arrays.hashCode(ff) : 0);
            result = 31 * result + (dd != null ? Arrays.hashCode(dd) : 0);
            result = 31 * result + (nn != null ? Arrays.hashCode(nn) : 0);
            return result;
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    public static class NamedPortable implements Portable {
        static final int CLASS_ID = 3;

        String name;
        int k;

        private NamedPortable() {
        }

        private NamedPortable(String name, int k) {
            this.name = name;
            this.k = k;
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("name", name);
            writer.writeInt("myint", k);
        }

        public void readPortable(PortableReader reader) throws IOException {
            k = reader.readInt("myint");
            name = reader.readUTF("name");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NamedPortable that = (NamedPortable) o;

            if (k != that.k) return false;
            if (name != null ? !name.equals(that.name) : that.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + k;
            return result;
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("NamedPortable{");
            sb.append("name='").append(name).append('\'');
            sb.append(", k=").append(k);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class NamedPortableV2 extends NamedPortable implements Portable {

        private int v;

        private NamedPortableV2() {
        }

        private NamedPortableV2(int v) {
            this.v = v;
        }

        private NamedPortableV2(String name, int v) {
            super(name, v * 10);
            this.v = v;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            super.writePortable(writer);
            writer.writeInt("v", v);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            super.readPortable(reader);
            v = reader.readInt("v");
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("NamedPortableV2{");
            sb.append("name='").append(name).append('\'');
            sb.append(", k=").append(k);
            sb.append(", v=").append(v);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class RawDataPortable implements Portable {
        static final int CLASS_ID = 4;

        long l;
        char[] c;
        NamedPortable p;
        int k;
        String s;
        SimpleDataSerializable sds;

        private RawDataPortable() {
        }

        private RawDataPortable(long l, char[] c, NamedPortable p, int k, String s, SimpleDataSerializable sds) {
            this.l = l;
            this.c = c;
            this.p = p;
            this.k = k;
            this.s = s;
            this.sds = sds;
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("l", l);
            writer.writeCharArray("c", c);
            writer.writePortable("p", p);
            final ObjectDataOutput output = writer.getRawDataOutput();
            output.writeInt(k);
            output.writeUTF(s);
            output.writeObject(sds);
        }

        public void readPortable(PortableReader reader) throws IOException {
            l = reader.readLong("l");
            c = reader.readCharArray("c");
            p = reader.readPortable("p");
            final ObjectDataInput input = reader.getRawDataInput();
            k = input.readInt();
            s = input.readUTF();
            sds = input.readObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RawDataPortable that = (RawDataPortable) o;

            if (k != that.k) return false;
            if (l != that.l) return false;
            if (!Arrays.equals(c, that.c)) return false;
            if (p != null ? !p.equals(that.p) : that.p != null) return false;
            if (s != null ? !s.equals(that.s) : that.s != null) return false;
            if (sds != null ? !sds.equals(that.sds) : that.sds != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (l ^ (l >>> 32));
            result = 31 * result + (c != null ? Arrays.hashCode(c) : 0);
            result = 31 * result + (p != null ? p.hashCode() : 0);
            result = 31 * result + k;
            result = 31 * result + (s != null ? s.hashCode() : 0);
            result = 31 * result + (sds != null ? sds.hashCode() : 0);
            return result;
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    public static class InvalidRawDataPortable extends RawDataPortable {
        static final int CLASS_ID = 5;

        private InvalidRawDataPortable() {
        }

        private InvalidRawDataPortable(long l, char[] c, NamedPortable p, int k, String s, SimpleDataSerializable sds) {
            super(l, c, p, k, s, sds);
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("l", l);
            final ObjectDataOutput output = writer.getRawDataOutput();
            output.writeInt(k);
            output.writeUTF(s);
            writer.writeCharArray("c", c);
            output.writeObject(sds);
            writer.writePortable("p", p);
        }
    }

    public static class InvalidRawDataPortable2 extends RawDataPortable {
        static final int CLASS_ID = 6;

        private InvalidRawDataPortable2() {
        }

        private InvalidRawDataPortable2(long l, char[] c, NamedPortable p, int k, String s, SimpleDataSerializable sds) {
            super(l, c, p, k, s, sds);
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void readPortable(PortableReader reader) throws IOException {
            c = reader.readCharArray("c");
            final ObjectDataInput input = reader.getRawDataInput();
            k = input.readInt();
            l = reader.readLong("l");
            s = input.readUTF();
            p = reader.readPortable("p");
            sds = input.readObject();
        }
    }

    public static class SimpleDataSerializable implements DataSerializable {
        private byte[] data;

        private SimpleDataSerializable() {
        }

        private SimpleDataSerializable(byte[] data) {
            this.data = data;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(data.length);
            out.write(data);
        }

        public void readData(ObjectDataInput in) throws IOException {
            int len = in.readInt();
            data = new byte[len];
            in.readFully(data);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SimpleDataSerializable that = (SimpleDataSerializable) o;

            if (!Arrays.equals(data, that.data)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return data != null ? Arrays.hashCode(data) : 0;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SimpleDataSerializable{");
            sb.append("data=").append(Arrays.toString(data));
            sb.append('}');
            return sb.toString();
        }
    }

    public static class ComplexDataSerializable implements DataSerializable {

        private SimpleDataSerializable ds;
        private NamedPortable portable;
        private SimpleDataSerializable ds2;

        private ComplexDataSerializable() {
        }

        private ComplexDataSerializable(NamedPortable portable, SimpleDataSerializable ds, SimpleDataSerializable ds2) {
            this.portable = portable;
            this.ds = ds;
            this.ds2 = ds2;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            ds.writeData(out);
            out.writeObject(portable);
            ds2.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            ds = new SimpleDataSerializable();
            ds.readData(in);
            portable = in.readObject();
            ds2 = new SimpleDataSerializable();
            ds2.readData(in);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ComplexDataSerializable that = (ComplexDataSerializable) o;

            if (ds != null ? !ds.equals(that.ds) : that.ds != null) return false;
            if (ds2 != null ? !ds2.equals(that.ds2) : that.ds2 != null) return false;
            if (portable != null ? !portable.equals(that.portable) : that.portable != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = ds != null ? ds.hashCode() : 0;
            result = 31 * result + (portable != null ? portable.hashCode() : 0);
            result = 31 * result + (ds2 != null ? ds2.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ComplexDataSerializable{");
            sb.append("ds=").append(ds);
            sb.append(", portable=").append(portable);
            sb.append(", ds2=").append(ds2);
            sb.append('}');
            return sb.toString();
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testGenericPortable_whenMultipleTypesAreUsed() {
        SerializationService ss = new SerializationServiceBuilder()
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
                        }
                        throw new IllegalArgumentException();
                    }
                }).build();


        ss.toData(new ParentGenericPortable<ChildGenericPortable1>(new ChildGenericPortable1("aaa", "bbb")));

        Data data = ss.toData(new ParentGenericPortable<ChildGenericPortable2>(new ChildGenericPortable2("ccc")));
        ss.toObject(data);
    }

    private static class ParentGenericPortable<T extends Portable> implements Portable {
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

    private static class ChildGenericPortable1 implements Portable {
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
            writer.writeUTF("s1", s1);
            writer.writeUTF("s2", s2);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            s1 = reader.readUTF("s1");
            s2 = reader.readUTF("s2");
        }
    }

    private static class ChildGenericPortable2 implements Portable {
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
            writer.writeUTF("s", s);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            s = reader.readUTF("s");
        }
    }


}
