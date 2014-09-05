package com.hazelcast.nio.serialization;

import java.io.IOException;
import java.util.Arrays;

/**
* @author mdogan 22/05/14
*/
class InnerPortable implements Portable {

    static final int CLASS_ID = 2;

    byte[] bb;
    char[] cc;
    short[] ss;
    int[] ii;
    long[] ll;
    float[] ff;
    double[] dd;
    NamedPortable[] nn;

    InnerPortable() {
    }

    InnerPortable(byte[] bb, char[] cc, short[] ss, int[] ii, long[] ll, float[] ff, double[] dd, NamedPortable[] nn) {
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
        return PortableTest.FACTORY_ID;
    }
}
