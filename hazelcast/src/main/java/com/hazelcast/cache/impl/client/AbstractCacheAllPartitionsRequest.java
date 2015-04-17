/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Abstract Cache All Partitions request to handle InMemoryFormat which needed for operation provider
 */
abstract class AbstractCacheAllPartitionsRequest extends AllPartitionsClientRequest {

    protected String name;

    protected AbstractCacheAllPartitionsRequest() {
    }

    protected AbstractCacheAllPartitionsRequest(String name) {
        this.name = name;
    }

    protected CacheOperationProvider getOperationProvider() {
        ICacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        if (cacheConfig == null) {
            throw new CacheNotExistsException(
                    "Cache config for cache " + name + " has not been created yet !");
        }
        return service.getCacheOperationProvider(name, cacheConfig.getInMemoryFormat());
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");

    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

}
