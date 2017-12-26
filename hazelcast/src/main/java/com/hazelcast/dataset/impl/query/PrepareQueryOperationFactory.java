package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class PrepareQueryOperationFactory implements OperationFactory {

    private String preparationId;
    private String name;
    private Predicate predicate;

    public PrepareQueryOperationFactory() {
    }

    public PrepareQueryOperationFactory(String name, String preparationId, Predicate predicate) {
        this.name = name;
        this.preparationId = preparationId;
        this.predicate = predicate;
    }

    @Override
    public Operation createOperation() {
        return new PrepareQueryOperation(name, preparationId, predicate);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.PREPARE_QUERY_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        preparationId = in.readUTF();
        predicate = in.readObject();
    }
}
