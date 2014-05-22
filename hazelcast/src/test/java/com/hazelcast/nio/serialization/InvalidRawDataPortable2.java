package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;

/**
* @author mdogan 22/05/14
*/
class InvalidRawDataPortable2 extends RawDataPortable {
    static final int CLASS_ID = 6;

    InvalidRawDataPortable2() {
    }

    InvalidRawDataPortable2(long l, char[] c, NamedPortable p, int k, String s, SimpleDataSerializable sds) {
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
