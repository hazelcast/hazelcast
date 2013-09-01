package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 8/31/13
 */
public class CollectionClearBackupOperation extends CollectionOperation implements BackupOperation {
    
    Set<Long> itemIdSet;

    public CollectionClearBackupOperation() {
    }

    public CollectionClearBackupOperation(String name, Set<Long> itemIdSet) {
        super(name);
        this.itemIdSet = itemIdSet;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().clearBackup(itemIdSet);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(itemIdSet.size());
        for (Long itemId : itemIdSet) {
            out.writeLong(itemId);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        itemIdSet = new HashSet<Long>(size);
        for (int i=0; i<size; i++){
            itemIdSet.add(in.readLong());
        }
    }
}
