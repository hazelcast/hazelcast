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

import com.hazelcast.config.MultiMapConfig.ValueCollectionType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * @ali 1/3/13
 */
public class MultiMapCollectionResponse implements DataSerializable {

    private Collection collection;

    private ValueCollectionType collectionType;

    private transient boolean binary;

    private transient SerializationService serializationService;

    public MultiMapCollectionResponse() {
    }

    public MultiMapCollectionResponse(Collection collection, ValueCollectionType collectionType, boolean binary, SerializationService serializationService) {
        this.collection = collection;
        this.collectionType = collectionType;
        this.binary = binary;
        this.serializationService = serializationService;
    }

    public Collection<Data> getDataCollection(SerializationService serializationService) {
        if (collection == null){
            return getCollection();
        }
        Collection coll = null;
        boolean init = true;
        for (Object obj : collection) {
            if (init){
                if (obj instanceof Data){
                    return collection;
                }
                else {
                    coll = createCollection(collection.size());
                    init = false;
                }
            }
            coll.add(serializationService.toData(obj));
        }
        return coll;
    }

    public Collection getCollection(){
        if (collection == null){
            collection = createCollection(0);
        }
        return collection;
    }

    private Collection createCollection(int size){
        if (collectionType == ValueCollectionType.SET) {
            return  new HashSet(size);
        } else if (collectionType == ValueCollectionType.LIST) {
            return new LinkedList();
        }
        return null;
    }

    public Collection getObjectCollection(SerializationService serializationService) {
        if (collection == null){
            return getCollection();
        }
        Collection coll = null;
        boolean init = true;
        for (Object obj : collection) {
            if (init){
                if (obj instanceof Data){
                    coll = createCollection(collection.size());
                    init = false;
                }
                else {
                    return collection;
                }
            }
            coll.add(serializationService.toObject((Data)obj));
        }
        return coll;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(collectionType.toString());
        if (collection == null){
            out.writeInt(-1);
            return;
        }
        out.writeInt(collection.size());
        for (Object obj : collection) {
            Data data = binary ? (Data)obj : serializationService.toData(obj);
            data.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        collectionType = ValueCollectionType.valueOf(in.readUTF());
        int size = in.readInt();
        if (size == -1){
            return;
        }
        collection = createCollection(size);
        for (int i = 0; i < size; i++) {
            collection.add(IOUtil.readData(in));
        }
    }

}
