package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.*;

/**
 * @ali 9/1/13
 */
public class CollectionAddAllOperation extends CollectionBackupAwareOperation {

    protected List<Data> valueList;

    protected transient Map<Long, Data> valueMap;

    public CollectionAddAllOperation() {
    }

    public CollectionAddAllOperation(String name, List<Data> valueList) {
        super(name);
        this.valueList = valueList;
    }

    public boolean shouldBackup() {
        return valueMap != null && !valueMap.isEmpty();
    }

    public Operation getBackupOperation() {
        return new CollectionAddAllBackupOperation(name, valueMap);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_ALL;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        valueMap = getOrCreateContainer().addAll(valueList);
        response = !valueMap.isEmpty();
    }

    public void afterRun() throws Exception {
        for (Data value : valueMap.values()) {
            publishEvent(ItemEventType.ADDED, value);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueList.size());
        for (Data value : valueList) {
            value.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueList = new ArrayList<Data>(size);
        for (int i=0; i<size; i++){
            final Data value = new Data();
            value.readData(in);
            valueList.add(value);
        }
    }
}
