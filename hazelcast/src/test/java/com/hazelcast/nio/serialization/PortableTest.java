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

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @mdogan 1/4/13
 */
public class PortableTest {

    @Test
    public void test() {
        final SerializationService serializationService = new SerializationServiceImpl(1, new TestPortableFactory());
        final SerializationService serializationService2 = new SerializationServiceImpl(2, new TestPortableFactory());
        Data data;

        NamedPortable[] nn = new NamedPortable[5];
        for (int i = 0; i < nn.length; i++) {
            nn[i] = new NamedPortable("named-portable-" + i);
        }

        NamedPortable np = nn[0];
        data = serializationService.toData(np);
        Assert.assertEquals(np, serializationService.toObject(data));
        Assert.assertEquals(np, serializationService2.toObject(data));

        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        data = serializationService.toData(inner);
        Assert.assertEquals(inner, serializationService.toObject(data));
        Assert.assertEquals(inner, serializationService2.toObject(data));

        MainPortable main = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        data = serializationService.toData(main);
        Assert.assertEquals(main, serializationService.toObject(data));
        Assert.assertEquals(main, serializationService2.toObject(data));
    }

    @Test
    public void testDifferentVersions() {
        final SerializationService serializationService = new SerializationServiceImpl(1, new PortableFactory() {
            public Portable create(int classId) {
                return new NamedPortable();
            }
        });
        final SerializationService serializationService2 = new SerializationServiceImpl(2, new PortableFactory() {
            public Portable create(int classId) {
                return new NamedPortableV2();
            }
        });

        NamedPortable p1 = new NamedPortable("portable-v1");
        Data data = serializationService.toData(p1);

        NamedPortableV2 p2 = new NamedPortableV2("portable-v2", 123);
        Data data2 = serializationService2.toData(p2);

        serializationService2.toObject(data);
        serializationService.toObject(data2);
    }


    private class TestPortableFactory implements PortableFactory {

        public Portable create(int classId) {
            switch (classId) {
                case 0:
                    return new MainPortable();
                case 1:
                    return new InnerPortable();
                case 2:
                    return new NamedPortable();
            }
            return null;
        }
    }

    private static class MainPortable implements Portable {

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
            return 0;
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
            writer.writePortable("p", p);
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
    }

    private static class InnerPortable implements Portable {

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
            return 1;
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
    }

    private static class NamedPortable implements Portable {

        String name;

        private NamedPortable() {
        }

        private NamedPortable(String name) {
            this.name = name;
        }

        public int getClassId() {
            return 2;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("name", name);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readUTF("name");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NamedPortable that = (NamedPortable) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    private static class NamedPortableV2 extends NamedPortable implements Portable {

        private int v;

        private NamedPortableV2() {
        }

        private NamedPortableV2(int v) {
            this.v = v;
        }

        private NamedPortableV2(String name, int v) {
            super(name);
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
    }


}
