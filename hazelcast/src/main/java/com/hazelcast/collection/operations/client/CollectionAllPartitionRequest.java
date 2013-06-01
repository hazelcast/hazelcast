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

import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @ali 5/10/13
 */
public abstract class CollectionAllPartitionRequest extends AllPartitionsClientRequest implements Portable {

    CollectionProxyId proxyId;

    protected CollectionAllPartitionRequest() {
    }

    protected CollectionAllPartitionRequest(CollectionProxyId proxyId) {
        this.proxyId = proxyId;
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        final ObjectDataOutput out = writer.getRawDataOutput();
        proxyId.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        final ObjectDataInput in = reader.getRawDataInput();
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
    }
}
