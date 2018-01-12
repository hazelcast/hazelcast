package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.function.Consumer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ProjectionOperation extends DataSetOperation {

    private Map<String, Object> bindings;
    private String compileId;
    private Consumer consumer;
    private String collectionClass;
    private transient Collection result;

    public ProjectionOperation() {
    }

    public ProjectionOperation(String name, String compileId, Map<String, Object> bindings, String collectionClazz) {
        super(name);
        this.compileId = compileId;
        this.bindings = bindings;
        this.collectionClass = collectionClazz;
    }

    @Override
    public void run() throws Exception {
        Consumer consumer = this.consumer;
        if(consumer == null){
            result = (Collection)getClass().getClassLoader().loadClass(collectionClass).newInstance();
            consumer = new Consumer() {
                @Override
                public void accept(Object o) {
                    result.add(o);
                }
            };
        }

        partition.projection(compileId, bindings, consumer);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.PROJECTION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(compileId);
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

        compileId = in.readUTF();
        collectionClass = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}