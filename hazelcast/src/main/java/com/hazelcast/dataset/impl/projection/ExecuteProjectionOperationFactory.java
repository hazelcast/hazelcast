package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecuteProjectionOperationFactory implements OperationFactory {

    private String name;
    private String preparationId;
    private Map<String, Object> bindings;
    private String collectionClass;

    public ExecuteProjectionOperationFactory() {
    }

    public ExecuteProjectionOperationFactory(String name, String preparationId, Map<String, Object> bindings,
                                             Class collectionClass) {
        this.name = name;
        this.preparationId = preparationId;
        this.bindings = bindings;
        this.collectionClass = collectionClass.getName();
    }

    @Override
    public Operation createOperation() {
        return new ExecuteProjectionOperation(name, preparationId, bindings, collectionClass);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.EXECUTE_PROJECTION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
        out.writeUTF(collectionClass);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        preparationId = in.readUTF();
        collectionClass = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
