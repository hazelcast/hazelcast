package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * @ali 8/31/13
 */
public class ListSetOperation extends CollectionBackupAwareOperation {

    private int index;

    private Data value;

    private transient long itemId = -1;

    private transient long oldItemId = -1;

    public ListSetOperation() {
    }

    public ListSetOperation(String name, int index, Data value) {
        super(name);
        this.index = index;
        this.value = value;
    }

    public boolean shouldBackup() {
        return oldItemId != -1;
    }

    public Operation getBackupOperation() {
        return new ListSetBackupOperation(name, oldItemId, itemId, value);
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_SET_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        final ListContainer container = getOrCreateListContainer();
        itemId = container.nextId();
        final CollectionItem item = container.set(index, itemId, value);
        oldItemId = item.getItemId();
        response = item.getValue();
    }

    public void afterRun() throws Exception {

    }
}
