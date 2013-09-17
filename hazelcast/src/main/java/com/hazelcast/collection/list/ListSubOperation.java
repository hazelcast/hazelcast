package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializableCollection;

import java.io.IOException;
import java.util.List;

/**
 * @ali 9/2/13
 */
public class ListSubOperation extends CollectionOperation {

    private int from;
    private int to;

    public ListSubOperation() {
    }

    public ListSubOperation(String name, int from, int to) {
        super(name);
        this.from = from;
        this.to = to;
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_SUB;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        final List<Data> sub = getOrCreateListContainer().sub(from, to);
        response = new SerializableCollection(sub);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(from);
        out.writeInt(to);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        from = in.readInt();
        to = in.readInt();
    }
}
