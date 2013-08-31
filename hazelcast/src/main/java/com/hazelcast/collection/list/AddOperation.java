package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 8/30/13
 */
public class AddOperation extends CollectionBackupAwareOperation {

    private int index = -1;

    private Data value;

    private transient long itemId = -1;

    public AddOperation() {
    }

    public AddOperation(String name, int index, Data value) {
        super(name);
        this.index = index;
        this.value = value;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        final ListContainer container = getOrCreateListContainer();
        final CollectionItem item = container.add(index, value);
        if (item != null){
            itemId = item.getItemId();
            response = true;
        } else {
            response = false;
        }
    }

    public void afterRun() throws Exception {

    }

    public Operation getBackupOperation() {
        return new AddBackupOperation(name, itemId, value);
    }

    public boolean shouldBackup() {
        return itemId != -1;
    }

    public int getId() {
        return CollectionDataSerializerHook.ADD;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        value = new Data();
        value.readData(in);
    }

}
