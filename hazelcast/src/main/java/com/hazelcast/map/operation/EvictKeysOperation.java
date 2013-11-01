package com.hazelcast.map.operation;

import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * User: ahmetmircik
 * Date: 11/1/13
 */
public class EvictKeysOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {
    Set<Data> keys;
    boolean shouldBackup = true;

    public EvictKeysOperation() {
    }

    public EvictKeysOperation(String name, Set<Data> keys) {
        super(name);
        this.keys = keys;
    }

    public void run() {
        final RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);

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
        EvictKeysBackupOperation evictKeysBackupOperation = new EvictKeysBackupOperation(name, keys);
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
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }
}
