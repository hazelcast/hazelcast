/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

import java.io.IOException;

public class GetOperation extends AbstractNamedOperation implements IdentifiedDataSerializable, ReadonlyOperation {

    private Data key;
    private transient VectorDocument<Data> result;

    public GetOperation() {
    }

    public GetOperation(String vectorCollectionName, Data key) {
        super(vectorCollectionName);
        this.key = key;
    }

    @Override
    public void run() throws Exception {
        VectorCollectionService service = getService();
        var storage = service.getStorageOrNull(getName(), getPartitionId());
        if (storage != null) {
            result = storage.get(key);
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public String getServiceName() {
        return VectorCollectionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.GET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.key = IOUtil.readData(in);
    }
}
