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
import com.hazelcast.collection.operations.MultiMapOperationFactory;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @ali 5/10/13
 */
public class ContainsEntryRequest extends CollectionAllPartitionRequest implements RetryableRequest {

    Data key;

    Data value;

    public ContainsEntryRequest() {
    }

    public ContainsEntryRequest(CollectionProxyId proxyId, Data key, Data value) {
        super(proxyId);
        this.key = key;
        this.value = value;
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(proxyId, MultiMapOperationFactory.OperationFactoryType.CONTAINS, key, value);
    }

    protected Object reduce(Map<Integer, Object> map) {
        for (Object obj: map.values()){
            if (Boolean.TRUE.equals(obj)){
                return true;
            }
        }
        return false;
    }

    public int getClassId() {
        return CollectionPortableHook.CONTAINS_ENTRY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
        super.writePortable(writer);
    }

    public void readPortable(PortableReader reader) throws IOException {
        final ObjectDataInput in = reader.getRawDataInput();
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
        super.readPortable(reader);
    }
}
