package com.hazelcast.map.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

/**
 * date: 9/16/13
 * author: eminn
 */
public class PartitionWideEntryWithPredicateOperation extends  AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation{
    EntryProcessor entryProcessor;
    MapEntrySet response;
    Predicate predicate;

    public PartitionWideEntryWithPredicateOperation(){}

    public PartitionWideEntryWithPredicateOperation(String name, EntryProcessor entryProcessor, Predicate predicate) {
        super(name);
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    public void run() {
        response = new MapEntrySet();
        QueryEntry queryEntry;
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        Map<Data, Record> records = recordStore.getRecords();
        for (Map.Entry<Data, Record> recordEntry : records.entrySet()) {
            Data dataKey = recordEntry.getKey();
            Record record = recordEntry.getValue();
            queryEntry = new QueryEntry(getNodeEngine().getSerializationService(),dataKey,mapService.toObject(record.getKey()),mapService.toObject(record.getValue()));
            Object result;
            if(predicate.apply(queryEntry))
            {
               Map.Entry mapEntry = new AbstractMap.SimpleEntry(mapService.toObject(record.getKey()),mapService.toObject(record.getValue()));
               result = entryProcessor.process(mapEntry);
            }
            else
              continue;

            if (result != null) {
                response.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, mapService.toData(result)));
            }

            if (queryEntry.getValue() == null) {
                recordStore.remove(dataKey);
            } else {
                recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, queryEntry.getValue()));
            }
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
    public Object getResponse() {
        return response;
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
    public String toString() {
        return "PartitionWideEntryWithPredicateOperation{}";
    }

    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    public int getSyncBackupCount() {
        return 0;
    }

    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new PartitionWideEntryBackupOperation(name, backupProcessor) : null;
    }
}
