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

package com.hazelcast.queue.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @ali 5/14/13
 */
public class PortableCollectionContainer implements Portable {

    private Collection<Data> collection;

    public PortableCollectionContainer() {
    }

    public PortableCollectionContainer(Collection<Data> collection) {
        this.collection = collection;
    }

    public Collection<Data> getCollection() {
        return collection;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.COLLECTION_CONTAINER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("s",collection.size());
        final ObjectDataOutput out = writer.getRawDataOutput();
        for (Data data: collection){
            data.writeData(out);
        }
    }

    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("s");
        final ObjectDataInput in = reader.getRawDataInput();
        collection = new ArrayList<Data>(size);
        for (int i=0; i<size; i++){
            Data data = new Data();
            data.readData(in);
            collection.add(data);
        }
    }
}
