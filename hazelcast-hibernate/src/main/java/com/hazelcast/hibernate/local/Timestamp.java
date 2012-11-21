package com.hazelcast.hibernate.local;

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 11/19/12
 */
public class Timestamp implements DataSerializable {

    private Object key;
    private long timestamp;

    public Timestamp() {
    }

    public Timestamp(final Object key, final long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public Object getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void writeData(final DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, key);
        out.writeLong(timestamp);
    }

    public void readData(final DataInput in) throws IOException {
        key = SerializationHelper.readObject(in);
        timestamp = in.readLong();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Timestamp");
        sb.append("{key=").append(key);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }
}