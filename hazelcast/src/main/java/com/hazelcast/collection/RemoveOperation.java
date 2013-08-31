package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 8/31/13
 */
public class RemoveOperation extends CollectionBackupAwareOperation {

    private Data value;

    private transient long itemId = -1;

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data value) {
        super(name);
        this.value = value;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        response = false;
        final CollectionItem item = getOrCreateContainer().remove(value);
        if (item != null){
            response = true;
            itemId = item.getItemId();
        }
    }

    public void afterRun() throws Exception {

    }

    public boolean shouldBackup() {
        return itemId != -1;
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, itemId);
    }

    public int getId() {
        return CollectionDataSerializerHook.REMOVE;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = new Data();
        value.readData(in);
    }
}
