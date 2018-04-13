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

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

/**
 * Gets a cache configuration or creates one, if a matching cache config is found in this member's config.
 *
 * @see CacheCreateConfigOperation
 * @see AddCacheConfigOperation
 */
public class CacheGetConfigOperation extends AbstractNamedOperation implements IdentifiedDataSerializable, ReadonlyOperation {

    private transient volatile Object response;
    private transient ICompletableFuture createOnAllMembersFuture;
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
        AbstractCacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        if (cacheConfig == null) {
            cacheConfig = service.findCacheConfig(simpleName);
            if (cacheConfig != null) {
                cacheConfig.setManagerPrefix(name.substring(0, name.lastIndexOf(simpleName)));
                CacheConfig existingCacheConfig = service.putCacheConfigIfAbsent(cacheConfig);
                if (existingCacheConfig != null) {
                    cacheConfig = existingCacheConfig;
                } else {
                    // a new cache config was added on the local member, all members should become aware of it
                    createOnAllMembersFuture = service.createCacheConfigOnAllMembersAsync(PreJoinCacheConfig.of(cacheConfig));
                }
            }
        }
        response = cacheConfig;
        if (createOnAllMembersFuture != null) {
            createOnAllMembersFuture.andThen(new ExecutionCallback() {

                @Override
                public void onResponse(Object asyncResponse) {
                    CacheGetConfigOperation.this.sendResponse(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    CacheGetConfigOperation.this.sendResponse(t);
                }
            });
        }
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
    public boolean returnsResponse() {
        return createOnAllMembersFuture == null;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_CONFIG;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
