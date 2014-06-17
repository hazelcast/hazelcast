package com.hazelcast.map.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.MapEntrySimple;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * date: 19/12/13
 * author: eminn
 */
public class MultipleEntryBackupOperation extends AbstractMapOperation implements BackupOperation, PartitionAwareOperation {
    private Set<Data> keys;
    private EntryBackupProcessor backupProcessor;

    public MultipleEntryBackupOperation() {
    }

    public MultipleEntryBackupOperation(String name, Set<Data> keys, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
        this.keys = keys;
    }

    @Override
    public void run() throws Exception {
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        final RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        MapEntrySimple entry;

        for (Data key : keys) {
            if (partitionService.getPartitionId(key) != getPartitionId())
                continue;
            Object objectKey = mapService.toObject(key);
            final Map.Entry<Data, Object> mapEntry = recordStore.getMapEntry(key);
            final Object valueBeforeProcess = mapService.toObject(mapEntry.getValue());
            entry = new MapEntrySimple(objectKey, valueBeforeProcess);
            backupProcessor.processBackup(entry);
            if(!entry.isModified())
                continue;
            if (entry.getValue() == null ) {
                recordStore.remove(key);
            } else{
                recordStore.putBackup(key, entry.getValue());
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
        int size = in.readInt();
        keys = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = new Data();
            key.readData(in);
            keys.add(key);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            key.writeData(out);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "MultipleEntryBackupOperation{}";
    }

}
