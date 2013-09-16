package com.hazelcast.map.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * date: 9/16/13
 * author: eminn
 */
public class PartitionWideEntryWithPredicateOperationFactory implements OperationFactory {
    private String name;
    private EntryProcessor entryProcessor;
    private Predicate predicate;

    public PartitionWideEntryWithPredicateOperationFactory() {
    }

    public PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate) {
        this.name = name;
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    @Override
    public Operation createOperation() {
        return new PartitionWideEntryWithPredicateOperation(name, entryProcessor, predicate);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entryProcessor);
        out.writeObject(predicate);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entryProcessor = in.readObject();
        predicate = in.readObject();

    }
}
