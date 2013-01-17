package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 1/16/13
 */
public class ContainsAllOperation extends CollectionKeyBasedOperation {

    Set<Data> dataSet;

    public ContainsAllOperation() {
    }

    public ContainsAllOperation(String name, CollectionProxyType proxyType, Data dataKey, Set<Data> dataSet) {
        super(name, proxyType, dataKey);
        this.dataSet = dataSet;
    }

    public void run() throws Exception {
        Collection coll  = getCollection();
        if (coll != null){
            if (isBinary()){
                response = coll.containsAll(dataSet);
            }
            else {
                Set set = new HashSet(dataSet.size());
                for (Data data: dataSet){
                    set.add(toObject(data));
                }
                response = coll.containsAll(set);
            }
        }
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(dataSet.size());
        for (Data data: dataSet){
            data.writeData(out);
        }
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readData(in);
        int size = in.readInt();
        dataSet = new HashSet<Data>(size);
        for (int i=0; i<size; i++){
            Data data = new Data();
            data.readData(in);
            dataSet.add(data);
        }
    }
}
