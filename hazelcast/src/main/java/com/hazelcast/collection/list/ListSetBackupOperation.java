package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 8/31/13
 */
public class ListSetBackupOperation extends CollectionOperation implements BackupOperation {

    private long oldItemId;
    private long itemId;
    private Data value;

    public ListSetBackupOperation() {
    }

    public ListSetBackupOperation(String name, long oldItemId, long itemId, Data value) {
        super(name);
        this.oldItemId = oldItemId;
        this.itemId = itemId;
        this.value = value;
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_SET_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateListContainer().setBackup(oldItemId, itemId, value);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(oldItemId);
        out.writeLong(itemId);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        oldItemId = in.readLong();
        itemId = in.readLong();
        value = new Data();
        value.readData(in);
    }
}
