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
import com.hazelcast.collection.operations.ContainsAllOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 5/10/13
 */
public class ContainsAllRequest extends CollectionKeyBasedRequest implements RetryableRequest {

    Set<Data> dataSet;

    public ContainsAllRequest() {
    }

    public ContainsAllRequest(CollectionProxyId proxyId, Data key, Set<Data> dataSet) {
        super(proxyId, key);
        this.dataSet = dataSet;
    }

    protected Operation prepareOperation() {
        return new ContainsAllOperation(proxyId, key, dataSet);
    }

    public int getClassId() {
        return CollectionPortableHook.CONTAINS_ALL;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("s", dataSet.size());
        final ObjectDataOutput out = writer.getRawDataOutput();
        for (Data data: dataSet){
            data.writeData(out);
        }
        super.writePortable(writer);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("s");
        dataSet = new HashSet<Data>(size);
        final ObjectDataInput in = reader.getRawDataInput();
        for (int i=0; i<size; i++){
            Data data = new Data();
            data.readData(in);
            dataSet.add(data);
        }
        super.readPortable(reader);
    }
}
