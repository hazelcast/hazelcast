package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NewDataSetOperationFactory implements OperationFactory {

    private String recordType;
    private String srcName;
    private String targetName;
    private String compileId;
    private Map<String, Object> bindings;

    public NewDataSetOperationFactory() {
    }

    public NewDataSetOperationFactory(String srcName, String targetName, String recordType, String compileId, Map<String, Object> bindings) {
        this.recordType = recordType;
        this.srcName = srcName;
        this.targetName = targetName;
        this.compileId = compileId;
        this.bindings = bindings;
    }

    @Override
    public Operation createOperation() {
        return new NewDataSetOperation(srcName, targetName, recordType, compileId, bindings);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.NEW_DATASET_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(srcName);
        out.writeUTF(targetName);
        out.writeUTF(recordType);
        out.writeUTF(compileId);
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
        compileId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}