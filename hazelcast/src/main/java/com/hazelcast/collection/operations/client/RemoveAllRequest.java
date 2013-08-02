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

package com.hazelcast.collection.operations.client;

import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.collection.operations.RemoveAllOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PortableCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author ali 5/10/13
 */
public class RemoveAllRequest extends CollectionKeyBasedRequest implements InitializingObjectRequest {

    int threadId = -1;

    public RemoveAllRequest() {
    }

    public RemoveAllRequest(CollectionProxyId proxyId, Data key, int threadId) {
        super(proxyId, key);
        this.threadId = threadId;
    }

    protected Operation prepareOperation() {
        return new RemoveAllOperation(proxyId,key,threadId);
    }

    public int getClassId() {
        return CollectionPortableHook.REMOVE_ALL;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("t",threadId);
        super.writePortable(writer);
    }

    public void readPortable(PortableReader reader) throws IOException {
        threadId = reader.readInt("t");
        super.readPortable(reader);
    }

    protected Object filter(Object response) {
        if (response instanceof CollectionResponse){
            Collection<CollectionRecord> coll = ((CollectionResponse) response).getCollection();
            if (coll == null){
                return new PortableCollection();
            }
            Collection<Data> collection = new ArrayList<Data>(coll.size());
            for (CollectionRecord record: coll){
                collection.add(getClientEngine().toData(record.getObject()));
            }
            return new PortableCollection(collection);
        }
        return super.filter(response);
    }
}
