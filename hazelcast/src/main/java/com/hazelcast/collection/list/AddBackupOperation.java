package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.nio.serialization.Data;

/**
 * @ali 8/31/13
 */
public class AddBackupOperation extends CollectionOperation  {

    long itemId;

    Data value;

    public AddBackupOperation() {
    }

    public AddBackupOperation(String name, long itemId, Data value) {
        super(name);
        this.itemId = itemId;
        this.value = value;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateListContainer().addBackup(itemId, value);
    }

    public void afterRun() throws Exception {

    }

    public int getId() {
        return CollectionDataSerializerHook.ADD_BACKUP;
    }
}
