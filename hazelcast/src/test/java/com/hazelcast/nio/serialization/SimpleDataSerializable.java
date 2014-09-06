package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
* @author mdogan 22/05/14
*/
class SimpleDataSerializable implements DataSerializable {
    private byte[] data;

    SimpleDataSerializable() {
    }

    SimpleDataSerializable(byte[] data) {
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
