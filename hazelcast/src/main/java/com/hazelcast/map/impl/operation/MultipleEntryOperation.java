package com.hazelcast.map.impl.operation;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MultipleEntryOperation extends AbstractMultipleEntryOperation implements BackupAwareOperation {

    private Set<Data> keys;

    public MultipleEntryOperation() {
    }

    public MultipleEntryOperation(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        super(name, entryProcessor);
        this.keys = keys;
    }

    @Override
    public void innerBeforeRun() {
        super.innerBeforeRun();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    public void run() throws Exception {
        final long now = getNow();

        final Set<Data> keys = this.keys;
        for (Data dataKey : keys) {
            if (keyNotOwnedByThisPartition(dataKey)) {
                continue;
            }
            final Object oldValue = getValueFor(dataKey, now);

            final Object key = toObject(dataKey);
            final Object value = toObject(oldValue);

            final Map.Entry entry = createMapEntry(key, value);

            final Data response = process(entry);

            addToResponses(dataKey, response);

            // first call noOp, other if checks below depends on it.
            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemoved(entry, dataKey, oldValue, now)) {
                continue;
            }

            entryAddedOrUpdated(entry, dataKey, oldValue, now);
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public String toString() {
        return "MultipleEntryOperation{}";
    }

    @Override
    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new MultipleEntryBackupOperation(name, keys, backupProcessor) : null;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
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
        out.writeObject(entryProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            key.writeData(out);
        }
    }

}
