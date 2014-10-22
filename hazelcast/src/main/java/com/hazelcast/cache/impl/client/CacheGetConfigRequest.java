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
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * This client request  specifically calls {@link CacheGetConfigOperation} on the server side.
 * @see com.hazelcast.cache.impl.operation.CacheGetConfigOperation
 */
public class CacheGetConfigRequest
        extends AbstractCacheRequest {

    public CacheGetConfigRequest() {
    }

    public CacheGetConfigRequest(String cacheName) {
        super(cacheName);
    }

    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheGetConfigOperation(name);
    }

    public int getClassId() {
        return CachePortableHook.GET_CONFIG;
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
    }
}
