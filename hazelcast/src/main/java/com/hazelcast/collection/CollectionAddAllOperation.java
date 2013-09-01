package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ali 9/1/13
 */
public class CollectionAddAllOperation extends CollectionBackupAwareOperation {

    protected Set<Data> valueSet;

    protected transient Map<Long, Data> valueMap;

    public CollectionAddAllOperation() {
    }

    public CollectionAddAllOperation(String name, Set<Data> valueSet) {
        super(name);
        this.valueSet = valueSet;
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
        valueMap = getOrCreateContainer().addAll(valueSet);
        response = !valueMap.isEmpty();
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            value.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueSet = new HashSet<Data>(size);
        for (int i=0; i<size; i++){
            final Data value = new Data();
            value.readData(in);
            valueSet.add(value);
        }
    }
}
