package com.hazelcast.map.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MultipleEntryBackupOperation extends AbstractMultipleEntryOperation implements BackupOperation {

    private Set<Data> keys;

    public MultipleEntryBackupOperation() {
    }

    public MultipleEntryBackupOperation(String name, Set<Data> keys, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
        this.keys = keys;
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

            processBackup(entry);

            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemovedBackup(entry, dataKey)) {
                continue;
            }
            entryAddedOrUpdatedBackup(entry, dataKey);

            evict(true);
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
