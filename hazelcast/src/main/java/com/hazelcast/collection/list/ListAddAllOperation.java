package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionAddAllOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.List;

/**
 * @ali 9/1/13
 */
public class ListAddAllOperation extends CollectionAddAllOperation {

    private int index = -1;

    public ListAddAllOperation() {
    }

    public ListAddAllOperation(String name, int index, List<Data> valueList) {
        super(name, valueList);
        this.index = index;
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_ADD_ALL;
    }

    public void run() throws Exception {
        if (!hasEnoughCapacity(valueList.size())){
            response = false;
            return;
        }
        valueMap = getOrCreateListContainer().addAll(index, valueList);
        response = !valueMap.isEmpty();
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }
}
