package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;

/**
 * Operation which evicts all keys except locked ones.
 */
public class EvictAllOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    private boolean shouldRunOnBackup;

    private int numberOfEvictedEntries;

    public EvictAllOperation() {
    }

    public EvictAllOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {

        // TODO this also clears locked keys from near cache which should be preserved.
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        mapServiceContext.getNearCacheProvider().clearNearCache(name);

        final RecordStore recordStore = mapServiceContext.getExistingRecordStore(getPartitionId(), name);
        if (recordStore == null) {
            return;
        }
        numberOfEvictedEntries = recordStore.evictAll(false);
        shouldRunOnBackup = true;
    }

    @Override
    public boolean shouldBackup() {
        return shouldRunOnBackup;
    }

    @Override
    public Object getResponse() {
        return numberOfEvictedEntries;
    }

    @Override
    public int getSyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new EvictAllBackupOperation(name);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(numberOfEvictedEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        numberOfEvictedEntries = in.readInt();
    }

    @Override
    public String toString() {
        return "EvictAllOperation{"
                + "shouldRunOnBackup=" + shouldRunOnBackup
                + ", numberOfEvictedEntries=" + numberOfEvictedEntries
                + '}';
    }
}
