package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompilePredicateOperationFactory implements OperationFactory {

    private String compileId;
    private String name;
    private Predicate predicate;

    public CompilePredicateOperationFactory() {
    }

    public CompilePredicateOperationFactory(String name, String compileId, Predicate predicate) {
        this.name = name;
        this.compileId = compileId;
        this.predicate = predicate;
    }

    @Override
    public Operation createOperation() {
        return new CompilePredicateOperation(name, compileId, predicate);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.COMPILE_PREDICATE_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compileId);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compileId = in.readUTF();
        predicate = in.readObject();
    }
}
