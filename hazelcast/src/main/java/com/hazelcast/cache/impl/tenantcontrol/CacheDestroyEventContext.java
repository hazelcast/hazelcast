/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;

import javax.cache.Cache;
import java.io.IOException;

import static com.hazelcast.config.CacheConfigAccessor.setTenantControl;
import static com.hazelcast.spi.tenantcontrol.TenantControl.NOOP_TENANT_CONTROL;

public class CacheDestroyEventContext implements DestroyEventContext<Cache>, IdentifiedDataSerializable {

    private String cacheName;

    public CacheDestroyEventContext() {
    }

    public CacheDestroyEventContext(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public void destroy(Cache context) {
        if (context instanceof CacheProxy) {
            CacheProxy cache = (CacheProxy) context;
            CacheService cacheService = (CacheService) cache.getService();
            CacheConfig cacheConfig = cacheService.getCacheConfig(cache.getPrefixedName());
            setTenantControl(cacheConfig, NOOP_TENANT_CONTROL);
        }
    }

    @Override
    public Class<? extends Cache> getContextType() {
        return Cache.class;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_DESTROY_EVENT_CONTEXT;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(cacheName);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        cacheName = in.readUTF();
    }

    @Override
    public String getDistributedObjectName() {
        return cacheName;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }
}
