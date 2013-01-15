/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.processor.Entry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 1/14/13
 */
public class ContainsAllEntryProcessor  extends MultiMapEntryProcessor<Boolean>{

    Set<Data> dataSet;

    public ContainsAllEntryProcessor() {
    }

    public ContainsAllEntryProcessor(boolean binary, Set<Data> dataSet) {
        super(binary);
        this.dataSet = dataSet;
    }

    public Boolean execute(Entry entry) {
        Collection coll = entry.getValue();
        if (coll != null){
            if (isBinary()){
                return coll.containsAll(dataSet);
            }
            else {
                Set set = new HashSet(dataSet.size());
                for (Data data: dataSet){
                    set.add(entry.getSerializationService().toObject(data));
                }
                return coll.containsAll(set);
            }
        }
        return false;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(dataSet.size());
        for (Data data: dataSet){
            data.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
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
