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

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.GetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 5/10/13
 */
public class GetRequest extends CollectionKeyBasedRequest implements RetryableRequest {

    int index;

    public GetRequest() {
    }

    public GetRequest(CollectionProxyId proxyId, Data key, int index) {
        super(proxyId, key);
        this.index = index;
    }

    protected Operation prepareOperation() {
        return new GetOperation(proxyId,key,index);
    }

    public int getClassId() {
        return CollectionPortableHook.GET;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("i",index);
        super.writePortable(writer);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        index = reader.readInt("i");
        super.readPortable(reader);
    }
}
