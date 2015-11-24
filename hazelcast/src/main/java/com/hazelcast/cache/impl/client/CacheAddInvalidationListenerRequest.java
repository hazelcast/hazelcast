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

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.security.Permission;

public class CacheAddInvalidationListenerRequest extends BaseClientAddListenerRequest {

    private String name;

    public CacheAddInvalidationListenerRequest() {

    }

    public CacheAddInvalidationListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        ClientEndpoint endpoint = getEndpoint();
        ICacheService cacheService = getService();
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(name);
        CacheInvalidationListener listener = new CacheInvalidationListener(endpoint, getCallId(), cacheContext);
        String registrationId = cacheService.addInvalidationListener(name, listener, localOnly);
        endpoint.addListenerDestroyAction(ICacheService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ADD_INVALIDATION_LISTENER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}
