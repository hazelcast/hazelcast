package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.function.Consumer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ExecuteProjectionOperation extends DataSetOperation {

    private Map<String, Object> bindings;
    private String preparationId;
    private Consumer consumer;
    private String collectionClass;
    private transient Collection result;

    public ExecuteProjectionOperation() {
    }

    public ExecuteProjectionOperation(String name, String preparationId, Map<String, Object> bindings, String collectionClazz) {
        super(name);
        this.preparationId = preparationId;
        this.bindings = bindings;
        this.collectionClass = collectionClazz;
    }

    @Override
    public void run() throws Exception {
        Consumer consumer = this.consumer;
        if (consumer == null) {
            result = (Collection) getClass().getClassLoader().loadClass(collectionClass).newInstance();
            consumer = o -> result.add(o);
        }

        partition.executeProjection(preparationId, bindings, consumer);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.EXECUTE_PROJECTION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeUTF(collectionClass);
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
        collectionClass = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}