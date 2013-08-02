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
import com.hazelcast.collection.operations.RemoveIndexOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @author ali 5/10/13
 */
public class RemoveIndexRequest extends CollectionKeyBasedRequest implements InitializingObjectRequest {

    int index;

    int threadId;

    public RemoveIndexRequest() {
    }

    public RemoveIndexRequest(CollectionProxyId proxyId, Data key, int index, int threadId) {
        super(proxyId, key);
        this.index = index;
        this.threadId = threadId;
    }

    protected Operation prepareOperation() {
        return new RemoveIndexOperation(proxyId,key,threadId,index);
    }

    public int getClassId() {
        return CollectionPortableHook.REMOVE_INDEX;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("i",index);
        writer.writeInt("t",threadId);
        super.writePortable(writer);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        index = reader.readInt("i");
        threadId = reader.readInt("t");
        super.readPortable(reader);
    }
}
