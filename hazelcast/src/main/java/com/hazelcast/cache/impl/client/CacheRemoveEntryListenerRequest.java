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

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;

import java.security.Permission;

/**
 * Client request which unregisters the event listener on behalf of the client.
 *
 * @see com.hazelcast.cache.impl.CacheService#deregisterListener(String, String)
 */
public class CacheRemoveEntryListenerRequest
        extends BaseClientRemoveListenerRequest {

    public CacheRemoveEntryListenerRequest() {
    }

    public CacheRemoveEntryListenerRequest(String name) {
        super(name);
    }

    @Override
    protected boolean deRegisterListener() {
        final ICacheService service = getService();
        return service.deregisterListener(name, registrationId);
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.REMOVE_ENTRY_LISTENER;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "deregisterCacheEntryListener";
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

}
