package com.hazelcast.nio.serialization;

import java.io.IOException;

/**
* @author mdogan 22/05/14
*/
class MainPortable implements Portable {

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

    MainPortable() {
    }

    MainPortable(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str, InnerPortable p) {
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
            writer.writeNullPortable("p", PortableTest.FACTORY_ID, InnerPortable.CLASS_ID);
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
        return PortableTest.FACTORY_ID;
    }
}
