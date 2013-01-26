/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @ali 1/3/13
 */
public class CollectionResponse implements DataSerializable {

    private Collection<Data> collection;

    public CollectionResponse() {
    }

    public CollectionResponse(Collection<Data> collection) {
        if (collection == null) {
            return;
        }
        this.collection = collection;
    }

    public CollectionResponse(Collection<CollectionRecord> collection, NodeEngine nodeEngine) {
        if (collection == null) {
            return;
        }
        this.collection = new ArrayList<Data>(collection.size());
        for (CollectionRecord record : collection) {
            this.collection.add(nodeEngine.toData(record.getObject()));
        }
    }

    public Collection<Data> getCollection() {
        return collection;
    }

    public Collection getObjectCollection(NodeEngine nodeEngine) {
        if (collection == null) {
            return null;
        }
        Collection coll = new ArrayList(collection.size());
        for (Data data : collection) {
            coll.add(nodeEngine.toObject(data));
        }
        return coll;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        if (collection == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(collection.size());
        for (Data data : collection) {
            data.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        collection = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            collection.add(IOUtil.readData(in));
        }
    }

}
