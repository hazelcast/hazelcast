package com.hazelcast.map.operation;

import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;

/**
 * Operation which evicts all keys except locked ones.
 */
public class EvictAllBackupOperation extends AbstractMapOperation implements BackupOperation, DataSerializable {

    public EvictAllBackupOperation() {
    }

    public EvictAllBackupOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        final RecordStore recordStore = mapService.getExistingRecordStore(getPartitionId(), name);
        //if there is no recordStore, then there is nothing to evict.
        if (recordStore == null) {
            return;
        }
        recordStore.evictAllBackup();
    }

    @Override
    public String toString() {
        return "EvictAllBackupOperation{}";
    }
}
