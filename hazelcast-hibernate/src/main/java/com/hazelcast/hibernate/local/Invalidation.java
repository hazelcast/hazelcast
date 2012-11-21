package com.hazelcast.hibernate.local;

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 11/12/12
 */
public class Invalidation implements DataSerializable {

    private Object key;
    private Object version;

    public Invalidation() {
    }

    public Invalidation(final Object key, final Object version) {
        this.key = key;
        this.version = version;
    }

    public Object getKey() {
        return key;
    }

    public Object getVersion() {
        return version;
    }

    public void writeData(final DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, key);
        SerializationHelper.writeObject(out, version);
    }

    public void readData(final DataInput in) throws IOException {
        key = SerializationHelper.readObject(in);
        version = SerializationHelper.readObject(in);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Invalidation");
        sb.append("{key=").append(key);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }
}
