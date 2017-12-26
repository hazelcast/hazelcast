package com.hazelcast.dataseries.impl.projection;

import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecuteProjectionOperationFactory implements OperationFactory {

    private  boolean forkJoin;
    private String name;
    private String preparationId;
    private Map<String, Object> bindings;
    private String collectionClass;

    public ExecuteProjectionOperationFactory() {
    }

    public ExecuteProjectionOperationFactory(String name,
                                             String preparationId,
                                             Map<String, Object> bindings,
                                             Class collectionClass,
                                             boolean forkJoin) {
        this.name = name;
        this.preparationId = preparationId;
        this.bindings = bindings;
        this.collectionClass = collectionClass.getName();
        this.forkJoin = forkJoin;
    }

    @Override
    public Operation createOperation() {
        return new ExecuteProjectionOperation(name, preparationId, bindings, collectionClass,forkJoin);
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.EXECUTE_PROJECTION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
        out.writeBoolean(forkJoin);
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
        forkJoin = in.readBoolean();
        collectionClass = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
