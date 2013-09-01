package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ali 9/1/13
 */
public class CollectionAddAllBackupOperation extends CollectionOperation implements BackupOperation {

    protected Map<Long, Data> valueMap;

    public CollectionAddAllBackupOperation() {
    }

    public CollectionAddAllBackupOperation(String name, Map<Long, Data> valueMap) {
        super(name);
        this.valueMap = valueMap;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_ALL_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().addAllBackup(valueMap);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueMap.size());
        for (Map.Entry<Long, Data> entry : valueMap.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueMap = new HashMap<Long, Data>(size);
        for (int i=0; i<size; i++){
            final long itemId = in.readLong();
            final Data value = new Data();
            valueMap.put(itemId, value);
        }
    }
}
