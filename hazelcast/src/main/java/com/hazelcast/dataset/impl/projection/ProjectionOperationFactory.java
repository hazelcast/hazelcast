package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProjectionOperationFactory implements OperationFactory {

    private String name;
    private String compileId;
    private Map<String, Object> bindings;
    private String collectionClass;

    public ProjectionOperationFactory() {
    }

    public ProjectionOperationFactory(String name, String compileId, Map<String, Object> bindings,
                                      Class collectionClass) {
        this.name = name;
        this.compileId = compileId;
        this.bindings = bindings;
        this.collectionClass = collectionClass.getName();
    }

    @Override
    public Operation createOperation() {
        return new ProjectionOperation(name, compileId, bindings, collectionClass);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.PROJECTION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compileId);
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
        compileId = in.readUTF();
        collectionClass = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
