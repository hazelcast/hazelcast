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
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.security.Permission;

/**
 * Abstract cache request to handle a completion id registration.
 */
public abstract class AbstractCacheRequest
        extends KeyBasedClientRequest
        implements RetryableRequest {

    protected String name;

    protected InMemoryFormat inMemoryFormat;

    public AbstractCacheRequest() {
    }

    public AbstractCacheRequest(String name, InMemoryFormat inMemoryFormat) {
        this.name = name;
        this.inMemoryFormat = inMemoryFormat;
    }

    public final int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public final String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public void setCompletionId(Integer completionId) {
    }

    protected CacheOperationProvider getOperationProvider() {
        ICacheService service = getService();
        return service.getCacheOperationProvider(name, inMemoryFormat);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("i", inMemoryFormat.name());
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        inMemoryFormat = InMemoryFormat.valueOf(reader.readUTF("i"));
    }
}
