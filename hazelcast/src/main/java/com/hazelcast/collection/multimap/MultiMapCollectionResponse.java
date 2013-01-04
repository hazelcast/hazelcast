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

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * @ali 1/3/13
 */
public class MultiMapCollectionResponse implements DataSerializable {

    private Collection collection;

    private String collectionType;

    public MultiMapCollectionResponse() {
    }

    public MultiMapCollectionResponse(Collection collection, String collectionType) {
        this.collection = collection;
        this.collectionType = collectionType;
    }

    public Collection<Data> getDataCollection() {
        if (collection == null){
            if (collectionType.equals(MultiMapConfig.ValueCollectionType.SET.toString())) {
                collection = new HashSet(0);
            } else if (collectionType.equals(MultiMapConfig.ValueCollectionType.LIST.toString())) {
                collection = new LinkedList();
            }
        }
        return collection;
    }

    public Collection getCollection() {
        if (collection == null){
            return getDataCollection();
        }
        Collection coll = null;
        if (collectionType.equals(MultiMapConfig.ValueCollectionType.SET.toString())) {
            coll = new HashSet(collection.size());
        } else if (collectionType.equals(MultiMapConfig.ValueCollectionType.LIST.toString())) {
            coll = new LinkedList();
        }
        for (Object obj : collection) {
//            coll.add(IOUtil.toObject(obj));
        }
        return coll;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(collectionType);
        if (collection == null){
            out.writeInt(-1);
            return;
        }
        out.writeInt(collection.size());
        for (Object obj : collection) {
            out.writeObject(obj);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        collectionType = in.readUTF();
        int size = in.readInt();
        if (collectionType.equals(MultiMapConfig.ValueCollectionType.SET.toString())) {
            collection = new HashSet(Math.max(size, 0));
        } else if (collectionType.equals(MultiMapConfig.ValueCollectionType.LIST.toString())) {
            collection = new LinkedList();
        }
        for (int i = 0; i < size; i++) {
            collection.add(IOUtil.readNullableData(in));
        }
    }

}
