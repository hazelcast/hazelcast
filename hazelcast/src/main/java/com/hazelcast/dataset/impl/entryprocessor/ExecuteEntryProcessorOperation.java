package com.hazelcast.dataset.impl.entryprocessor;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecuteEntryProcessorOperation extends DataSetOperation {

    private Map<String, Object> bindings;
    private String preparationId;

    public ExecuteEntryProcessorOperation() {
    }

    public ExecuteEntryProcessorOperation(String name, String preparationId, Map<String, Object> bindings) {
        super(name);
        this.preparationId = preparationId;
        this.bindings = bindings;
    }

    @Override
    public void run() throws Exception {
        partition.executeEntryProcessor(preparationId, bindings);
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.EXECUTE_ENTRY_PROCESSOR_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(preparationId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        preparationId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}

