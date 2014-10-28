package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.HibernateDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.hibernate.cache.spi.access.SoftLock;

import java.io.IOException;
import java.util.Map;

/**
 * A concrete implementation of {@link com.hazelcast.map.EntryProcessor} which unlocks
 * a soft-locked region cached entry
 */
public class UnlockEntryProcessor extends AbstractRegionCacheEntryProcessor {

    private SoftLock lock;
    private String nextMarkerId;
    private long timestamp;

    public UnlockEntryProcessor() {
    }

    public UnlockEntryProcessor(SoftLock lock, String nextMarkerId, long timestamp) {
        this.lock = lock;
        this.nextMarkerId = nextMarkerId;
        this.timestamp = timestamp;
    }

    @Override
    public Void process(Map.Entry<Object, Expirable> entry) {
        Expirable expirable = entry.getValue();

        if (expirable != null) {
            if (expirable.matches(lock)) {
                expirable = ((ExpiryMarker) expirable).expire(timestamp);
            } else if (expirable.getValue() != null) {
                // It's a value. Expire the value immediately. This prevents
                // in-flight transactions from adding stale values to the cache
                expirable = new ExpiryMarker(null, timestamp, nextMarkerId).expire(timestamp);
            } else {
                // else its a different marker. Leave the it as is
                return null;
            }
            entry.setValue(expirable);
        }

        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(lock);
        out.writeUTF(nextMarkerId);
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        lock = in.readObject();
        nextMarkerId = in.readUTF();
        timestamp = in.readLong();
    }

    @Override
    public int getId() {
        return HibernateDataSerializerHook.UNLOCK;
    }

}
