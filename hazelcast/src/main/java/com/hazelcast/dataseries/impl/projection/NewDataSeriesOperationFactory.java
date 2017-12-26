package com.hazelcast.dataseries.impl.projection;

import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NewDataSeriesOperationFactory implements OperationFactory {

    private String recordType;
    private String srcName;
    private String targetName;
    private String preparationId;
    private Map<String, Object> bindings;

    public NewDataSeriesOperationFactory() {
    }

    public NewDataSeriesOperationFactory(String srcName,
                                         String targetName,
                                         String recordType,
                                         String preparationId,
                                         Map<String, Object> bindings) {
        this.recordType = recordType;
        this.srcName = srcName;
        this.targetName = targetName;
        this.preparationId = preparationId;
        this.bindings = bindings;
    }

    @Override
    public Operation createOperation() {
        return new NewDataSeriesOperation(srcName, targetName, recordType, preparationId, bindings);
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.NEW_DATASERIES_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(srcName);
        out.writeUTF(targetName);
        out.writeUTF(recordType);
        out.writeUTF(preparationId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        srcName = in.readUTF();
        targetName = in.readUTF();
        recordType = in.readUTF();
        preparationId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}