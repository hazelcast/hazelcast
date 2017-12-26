package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryOperation extends DataSetOperation {

    private Map<String, Object> bindings;
    private String compileId;
    private List result;

    public QueryOperation() {
    }

    public QueryOperation(String name, String compileId, Map<String, Object> bindings) {
        super(name);
        this.compileId = compileId;
        this.bindings = bindings;
    }

    @Override
    public void run() throws Exception {
        result = partition.query(compileId, bindings);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.QUERY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(compileId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        compileId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
