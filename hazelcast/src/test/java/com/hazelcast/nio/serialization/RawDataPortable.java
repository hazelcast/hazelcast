package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
* @author mdogan 22/05/14
*/
class RawDataPortable implements Portable {
    static final short CLASS_ID = 4;

    long l;
    char[] c;
    NamedPortable p;
    int k;
    String s;
    ByteArrayDataSerializable sds;

    RawDataPortable() {
    }

    RawDataPortable(long l, char[] c, NamedPortable p, int k, String s, ByteArrayDataSerializable sds) {
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
        return PortableTest.FACTORY_ID;
    }
}
