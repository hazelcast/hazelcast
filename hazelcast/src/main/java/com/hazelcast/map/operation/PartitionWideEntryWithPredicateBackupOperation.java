package com.hazelcast.map.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

/**
 * date: 9/16/13
 * author: eminn
 */
public class PartitionWideEntryWithPredicateBackupOperation extends AbstractMapOperation implements BackupOperation, PartitionAwareOperation {
    EntryBackupProcessor entryProcessor;
    Predicate predicate;

    public PartitionWideEntryWithPredicateBackupOperation() {
    }

    public PartitionWideEntryWithPredicateBackupOperation(String name, EntryBackupProcessor entryProcessor, Predicate predicate) {
        super(name);
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    public void run() {
        Map.Entry entry;
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        Map<Data,Record> records = recordStore.getRecords();
        for (Map.Entry<Data, Record> recordEntry : records.entrySet()) {
            Data dataKey = recordEntry.getKey();
            Record record = recordEntry.getValue();
            entry = new AbstractMap.SimpleEntry(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()));
            if(predicate.apply(entry))
                entryProcessor.processBackup(entry);
            else
                continue;
            recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, entry.getValue()));
        }
    }

    public void afterRun() throws Exception {
        super.afterRun();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
        predicate = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeObject(predicate);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "PartitionWideEntryWithPredicateBackupOperation{}";
    }

}
