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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * This client request  specifically calls {@link CacheContainsKeyOperation} on the server side.
 *
 * @see com.hazelcast.cache.impl.operation.CacheContainsKeyOperation
 */
public class CacheContainsKeyRequest
        extends AbstractCacheRequest {

    protected Data key;

    public CacheContainsKeyRequest() {
    }

    public CacheContainsKeyRequest(String name, Data key, InMemoryFormat inMemoryFormat) {
        super(name, inMemoryFormat);
        this.key = key;
    }

    public int getClassId() {
        return CachePortableHook.CONTAINS_KEY;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider();
        return operationProvider.createContainsKeyOperation(key);
    }

    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }
}
