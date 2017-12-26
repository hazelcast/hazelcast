package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecuteQueryOperationFactory implements OperationFactory {

    private String name;
    private String preparationId;
    private Map<String, Object> bindings;

    public ExecuteQueryOperationFactory() {
    }

    public ExecuteQueryOperationFactory(String name, String preparationId, Map<String, Object> bindings) {
        this.name = name;
        this.preparationId = preparationId;
        this.bindings = bindings;
    }

    @Override
    public Operation createOperation() {
        return new ExecuteQueryOperation(name, preparationId, bindings);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.EXECUTE_QUERY_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
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
        int size = in.readInt();
        bindings = new HashMap<String, Object>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
