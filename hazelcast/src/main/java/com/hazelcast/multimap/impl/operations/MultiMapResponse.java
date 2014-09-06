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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.multimap.impl.MultiMapContainerSupport.createCollection;
import static com.hazelcast.multimap.impl.MultiMapContainerSupport.emptyCollection;

public class MultiMapResponse implements DataSerializable {

    private Collection collection;

    private long nextRecordId = -1;

    private long txVersion = -1;

    private MultiMapConfig.ValueCollectionType collectionType
            = MultiMapConfig.DEFAULT_VALUE_COLLECTION_TYPE;

    public MultiMapResponse() {
    }

    public MultiMapResponse(Collection collection,
                            MultiMapConfig.ValueCollectionType collectionType) {
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

    public long getTxVersion() {
        return txVersion;
    }

    public MultiMapResponse setTxVersion(long txVersion) {
        this.txVersion = txVersion;
        return this;
    }

    public Collection getCollection() {
        return collection == null ? emptyCollection(collectionType) : collection;
    }

    public Collection getObjectCollection(NodeEngine nodeEngine) {
        if (collection == null) {
            return emptyCollection(collectionType);
        }
        final Collection newCollection = createCollection(collectionType, collection.size());
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
        final Collection<MultiMapRecord> newCollection
                = createCollection(collectionType, collection.size());
        for (Object obj : collection) {
            MultiMapRecord record = nodeEngine.toObject(obj);
            newCollection.add(record);
        }
        return newCollection;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(collectionType.name());
        out.writeLong(nextRecordId);
        out.writeLong(txVersion);
        if (collection == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(collection.size());
        for (Object obj : collection) {
            out.writeObject(obj);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        String collectionTypeName = in.readUTF();
        collectionType = MultiMapConfig.ValueCollectionType.valueOf(collectionTypeName);
        nextRecordId = in.readLong();
        txVersion = in.readLong();
        int size = in.readInt();
        if (size == -1) {
            collection = emptyCollection(collectionType);
            return;
        }
        collection = createCollection(collectionType, size);
        for (int i = 0; i < size; i++) {
            collection.add(in.readObject());
        }
    }
}
