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
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * This client request  specifically calls {@link CachePutOperation} on the server side.
 * @see com.hazelcast.cache.impl.operation.CachePutOperation
 */
public class CachePutRequest
        extends AbstractCacheRequest {

    protected Data key;
    protected Data value;
    protected ExpiryPolicy expiryPolicy;
    // getAndPut
    protected boolean get;
    private int completionId;

    public CachePutRequest() {
    }

    public CachePutRequest(String name, Data key, Data value, InMemoryFormat inMemoryFormat) {
        super(name, inMemoryFormat);
        this.key = key;
        this.value = value;
        this.expiryPolicy = null;
    }

    public CachePutRequest(String name, Data key, Data value, ExpiryPolicy expiryPolicy, InMemoryFormat inMemoryFormat) {
        super(name, inMemoryFormat);
        this.key = key;
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    public CachePutRequest(String name, Data key, Data value, ExpiryPolicy expiryPolicy,
                           boolean get, InMemoryFormat inMemoryFormat) {
        super(name, inMemoryFormat);
        this.key = key;
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = get;
    }

    public int getClassId() {
        return CachePortableHook.PUT;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider();
        return operationProvider.createPutOperation(key, value, expiryPolicy, get, completionId);
    }

    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeInt("c", completionId);
        writer.writeBoolean("g", get);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(value);
        out.writeObject(expiryPolicy);
    }

    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        completionId = reader.readInt("c");
        get = reader.readBoolean("g");
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
        value = in.readData();
        expiryPolicy = in.readObject();
    }

    public void setCompletionId(Integer completionId) {
        this.completionId = completionId != null ? completionId : -1;
    }
}
