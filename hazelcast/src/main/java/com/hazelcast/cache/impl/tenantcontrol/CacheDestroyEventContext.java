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

package com.hazelcast.cache.impl.tenantcontrol;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;

import java.io.IOException;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;

public class CacheDestroyEventContext implements DestroyEventContext<ICacheService>, IdentifiedDataSerializable {

    private String cacheDistributedObjectName;

    public CacheDestroyEventContext() {
    }

    public CacheDestroyEventContext(String cacheDistributedObjectName) {
        this.cacheDistributedObjectName = cacheDistributedObjectName;
    }

    @Override
    public void destroy(ICacheService context) {
        context.destroyDistributedObject(getDistributedObjectName(cacheDistributedObjectName));
    }

    @Override
    public Class<? extends ICacheService> getContextType() {
        return ICacheService.class;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CACHE_DESTROY_EVENT_CONTEXT;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(cacheDistributedObjectName);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        cacheDistributedObjectName = in.readUTF();
    }
}
