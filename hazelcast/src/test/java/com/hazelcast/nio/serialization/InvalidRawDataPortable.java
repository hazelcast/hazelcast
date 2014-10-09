package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
* @author mdogan 22/05/14
*/
class InvalidRawDataPortable extends RawDataPortable {
    static final int CLASS_ID = 5;

    InvalidRawDataPortable() {
    }

    InvalidRawDataPortable(long l, char[] c, NamedPortable p, int k, String s, ByteArrayDataSerializable sds) {
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
