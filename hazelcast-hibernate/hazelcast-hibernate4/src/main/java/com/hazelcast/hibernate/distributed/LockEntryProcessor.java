package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.HibernateDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;

/**
 * A concrete implementation of {@link com.hazelcast.map.EntryProcessor} which soft-locks
 * a region cached entry
 */
public class LockEntryProcessor extends AbstractRegionCacheEntryProcessor {

    private String nextMarkerId;
    private long timeout;
    private Object version;

    public LockEntryProcessor() {
    }

    public LockEntryProcessor(String nextMarkerId, long timeout, Object version) {
        this.nextMarkerId = nextMarkerId;
        this.timeout = timeout;
        this.version = version;
    }

    @Override
    public Expirable process(Map.Entry<Object, Expirable> entry) {
        Expirable expirable = entry.getValue();

        if (expirable == null) {
            expirable = new ExpiryMarker(version, timeout, nextMarkerId);
        } else {
            expirable = expirable.markForExpiration(timeout, nextMarkerId);
        }

        entry.setValue(expirable);

        return expirable;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(nextMarkerId);
        out.writeLong(timeout);
        out.writeObject(version);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nextMarkerId = in.readUTF();
        timeout = in.readLong();
        version = in.readObject();
    }

    @Override
    public int getId() {
        return HibernateDataSerializerHook.LOCK;
    }
}
