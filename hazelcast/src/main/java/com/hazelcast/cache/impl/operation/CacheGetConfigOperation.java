/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

/**
 * Cache GetConfig Operation.
 * <p>Gets the already created cache configuration from the related partition.</p>
 * @see CacheCreateConfigOperation
 */
public class CacheGetConfigOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation {

    private String simpleName;

    public CacheGetConfigOperation() {
    }

    public CacheGetConfigOperation(String name, String simpleName) {
        super(name);
        this.simpleName = simpleName;
    }

    @Override
    public void run()
            throws Exception {
        final ICacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        if (cacheConfig == null) {
            cacheConfig = service.findCacheConfig(simpleName);
            if (cacheConfig != null) {
                cacheConfig.setManagerPrefix(name.substring(0, name.lastIndexOf(simpleName)));
                CacheConfig existingCacheConfig = service.putCacheConfigIfAbsent(cacheConfig);
                if (existingCacheConfig != null) {
                    cacheConfig = existingCacheConfig;
                }
            }
        }
        response = cacheConfig;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(simpleName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        simpleName = in.readUTF();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_CONFIG;
    }
}
