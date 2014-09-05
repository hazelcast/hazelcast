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

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

public class CacheGetRequest
        extends AbstractCacheRequest {

    protected Data key;
    protected ExpiryPolicy expiryPolicy;

    public CacheGetRequest() {
    }

    public CacheGetRequest(String name, Data key) {
        super(name);
        this.key = key;
    }

    public CacheGetRequest(String name, Data key, ExpiryPolicy expiryPolicy) {
        super(name);
        this.key = key;
        this.expiryPolicy = expiryPolicy;
    }

    public int getClassId() {
        return CachePortableHook.GET;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheGetOperation(name, key, expiryPolicy);
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        out.writeObject(expiryPolicy);

    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        this.expiryPolicy = in.readObject();
    }

}
