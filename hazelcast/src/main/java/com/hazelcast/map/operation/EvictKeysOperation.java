package com.hazelcast.map.operation;

import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * User: ahmetmircik
 * Date: 11/1/13
 */
public class EvictKeysOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {
    MapKeySet mapKeySet;
    boolean shouldBackup = true;

    public EvictKeysOperation() {
    }

    public EvictKeysOperation(String name, Set<Data> keys) {
        super(name);
        this.mapKeySet = new MapKeySet(keys);
    }

    public void run() {
        final RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);

        final Set<Data> keys = mapKeySet.getKeySet();
        if (keys.isEmpty()) {
            shouldBackup = false;
        }

        for (Data key : keys) {
            if (!recordStore.isLocked(key)) {
                recordStore.evict(key);
            }
        }
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public int getSyncBackupCount() {
        return mapService.getMapContainer(name).getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapService.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    public Operation getBackupOperation() {
        EvictKeysBackupOperation evictKeysBackupOperation = new EvictKeysBackupOperation(name, mapKeySet.getKeySet());
        evictKeysBackupOperation.setServiceName(SERVICE_NAME);
        return evictKeysBackupOperation;
    }

    @Override
    public String toString() {
        return "EvictKeysOperation{" +
                '}';
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        mapKeySet.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapKeySet = new MapKeySet();
        mapKeySet.readData(in);
    }
}
