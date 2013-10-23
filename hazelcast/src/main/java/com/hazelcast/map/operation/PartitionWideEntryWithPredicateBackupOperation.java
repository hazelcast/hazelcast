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
public class PartitionWideEntryWithPredicateBackupOperation extends PartitionWideEntryBackupOperation {
    Predicate predicate;

    public PartitionWideEntryWithPredicateBackupOperation() {
    }

    public PartitionWideEntryWithPredicateBackupOperation(String name, EntryBackupProcessor entryProcessor, Predicate predicate) {
        super(name, entryProcessor);
        this.predicate = predicate;
    }


    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        predicate = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(predicate);
    }

    protected Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "PartitionWideEntryWithPredicateBackupOperation{}";
    }

}
