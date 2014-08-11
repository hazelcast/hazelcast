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

package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.client.client.RetryableRequest;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.util.Map;

public class ContainsEntryRequest extends MultiMapAllPartitionRequest implements RetryableRequest {

    Data key;

    Data value;

    public ContainsEntryRequest() {
    }

    public ContainsEntryRequest(String name, Data key, Data value) {
        super(name);
        this.key = key;
        this.value = value;
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(name, MultiMapOperationFactory.OperationFactoryType.CONTAINS, key, value);
    }

    protected Object reduce(Map<Integer, Object> map) {
        for (Object obj : map.values()) {
            if (Boolean.TRUE.equals(obj)) {
                return true;
            }
        }
        return false;
    }

    public int getClassId() {
        return MultiMapPortableHook.CONTAINS_ENTRY;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
    }

    @Override
    public String getMethodName() {
        if (key != null && value != null) {
            return "containsEntry";
        } else if (key != null) {
            return "containsKey";
        }
        return "containsValue";
    }

    @Override
    public Object[] getParameters() {
        if (key != null && value != null) {
            return new Object[]{key, value};
        } else if (key != null) {
            return new Object[]{key};
        }
        return new Object[]{value};
    }
}
