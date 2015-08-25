package com.hazelcast.internal.serialization;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @author mdogan 22/05/14
 */
class InvalidRawDataPortable extends RawDataPortable {
    InvalidRawDataPortable() {
    }

    InvalidRawDataPortable(long l, char[] c, NamedPortable p, int k, String s, ByteArrayDataSerializable sds) {
        super(l, c, p, k, s, sds);
    }

    public int getClassId() {
        return TestSerializationConstants.INVALID_RAW_DATA_PORTABLE;
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
