package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ali 9/2/13
 */
public class CollectionCompareAndRemoveOperation extends CollectionBackupAwareOperation {
    
    private boolean retain;
    
    private Set<Data> valueSet;

    private transient Map<Long, Data> itemIdMap;

    public CollectionCompareAndRemoveOperation() {
    }

    public CollectionCompareAndRemoveOperation(String name, boolean retain, Set<Data> valueSet) {
        super(name);
        this.retain = retain;
        this.valueSet = valueSet;
    }

    public boolean shouldBackup() {
        return !itemIdMap.isEmpty();
    }

    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdMap.keySet());
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_COMPARE_AND_REMOVE;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        itemIdMap = getOrCreateContainer().compareAndRemove(retain, valueSet);
        response = !itemIdMap.isEmpty();
    }

    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(retain);
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            value.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        retain = in.readBoolean();
        final int size = in.readInt();
        valueSet = new HashSet<Data>(size);
        for (int i=0; i<size; i++){
            final Data value = new Data();
            value.readData(in);
            valueSet.add(value);
        }
    }
}
