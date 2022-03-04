/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.multimap.impl.ValueCollectionFactory.createCollection;
import static com.hazelcast.multimap.impl.ValueCollectionFactory.emptyCollection;

public class MultiMapResponse implements IdentifiedDataSerializable {

    private long nextRecordId = -1;
    private MultiMapConfig.ValueCollectionType collectionType = MultiMapConfig.DEFAULT_VALUE_COLLECTION_TYPE;
    private Collection collection;

    public MultiMapResponse() {
    }

    public MultiMapResponse(Collection collection, MultiMapConfig.ValueCollectionType collectionType) {
        this.collection = collection;
        this.collectionType = collectionType;
    }

    public long getNextRecordId() {
        return nextRecordId;
    }

    public MultiMapResponse setNextRecordId(long recordId) {
        this.nextRecordId = recordId;
        return this;
    }

    public Collection getCollection() {
        return collection == null ? emptyCollection(collectionType) : collection;
    }

    public Collection getObjectCollection(NodeEngine nodeEngine) {
        if (collection == null) {
            return emptyCollection(collectionType);
        }
        Collection<Object> newCollection = createCollection(collectionType, collection.size());
        for (Object obj : collection) {
            MultiMapRecord record = nodeEngine.toObject(obj);
            newCollection.add(nodeEngine.toObject(record.getObject()));
        }
        return newCollection;
    }

    public Collection<MultiMapRecord> getRecordCollection(NodeEngine nodeEngine) {
        if (collection == null) {
            return emptyCollection(collectionType);
        }
        Collection<MultiMapRecord> newCollection = createCollection(collectionType, collection.size());
        for (Object obj : collection) {
            MultiMapRecord record = nodeEngine.toObject(obj);
            newCollection.add(record);
        }
        return newCollection;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(collectionType.name());
        out.writeLong(nextRecordId);
        if (collection == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(collection.size());
        for (Object obj : collection) {
            IOUtil.writeObject(out, obj);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        String collectionTypeName = in.readString();
        collectionType = MultiMapConfig.ValueCollectionType.valueOf(collectionTypeName);
        nextRecordId = in.readLong();
        int size = in.readInt();
        if (size == -1) {
            collection = emptyCollection(collectionType);
            return;
        }
        collection = createCollection(collectionType, size);
        for (int i = 0; i < size; i++) {
            collection.add(IOUtil.readObject(in));
        }
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MULTIMAP_RESPONSE;
    }
}
