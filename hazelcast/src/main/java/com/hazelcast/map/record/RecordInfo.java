package com.hazelcast.map.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @author enesakar 11/25/13
 */
public class RecordInfo implements DataSerializable {
    protected RecordStatistics statistics;
    protected long idleDelayMillis = -1;
    protected long ttlDelayMillis = -1;
    protected long mapStoreWriteDelayMillis = -1;
    protected long mapStoreDeleteDelayMillis = -1;

    public RecordInfo() {
    }

    public RecordInfo(RecordStatistics statistics,
                               long idleDelayMillis, long ttlDelayMillis, long mapStoreWriteDelayMillis,
                               long mapStoreDeleteDelayMillis) {
        this.statistics = statistics;
        this.idleDelayMillis = idleDelayMillis;
        this.ttlDelayMillis = ttlDelayMillis;
        this.mapStoreWriteDelayMillis = mapStoreWriteDelayMillis;
        this.mapStoreDeleteDelayMillis = mapStoreDeleteDelayMillis;
    }


    public RecordStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(RecordStatistics statistics) {
        this.statistics = statistics;
    }

    public long getIdleDelayMillis() {
        return idleDelayMillis;
    }

    public void setIdleDelayMillis(long idleDelayMillis) {
        this.idleDelayMillis = idleDelayMillis;
    }

    public long getTtlDelayMillis() {
        return ttlDelayMillis;
    }

    public void setTtlDelayMillis(long ttlDelayMillis) {
        this.ttlDelayMillis = ttlDelayMillis;
    }

    public long getMapStoreWriteDelayMillis() {
        return mapStoreWriteDelayMillis;
    }

    public void setMapStoreWriteDelayMillis(long mapStoreWriteDelayMillis) {
        this.mapStoreWriteDelayMillis = mapStoreWriteDelayMillis;
    }

    public long getMapStoreDeleteDelayMillis() {
        return mapStoreDeleteDelayMillis;
    }

    public void setMapStoreDeleteDelayMillis(long mapStoreDeleteDelayMillis) {
        this.mapStoreDeleteDelayMillis = mapStoreDeleteDelayMillis;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (statistics != null) {
            out.writeBoolean(true);
            statistics.writeData(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(idleDelayMillis);
        out.writeLong(ttlDelayMillis);
        out.writeLong(mapStoreWriteDelayMillis);
        out.writeLong(mapStoreDeleteDelayMillis);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        boolean statsEnabled = in.readBoolean();
        if (statsEnabled) {
            statistics = new RecordStatistics();
            statistics.readData(in);
        }
        idleDelayMillis = in.readLong();
        ttlDelayMillis = in.readLong();
        mapStoreWriteDelayMillis = in.readLong();
        mapStoreDeleteDelayMillis = in.readLong();
    }
}
