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

import java.security.Permission;

/**
 * Client request which unregisters the invalidation listener on behalf of the client.
 *
 */
public class CacheRemoveInvalidationListenerRequest
        extends BaseClientRemoveListenerRequest {

    public CacheRemoveInvalidationListenerRequest() {
    }

    public CacheRemoveInvalidationListenerRequest(String name) {
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
        return CachePortableHook.REMOVE_INVALIDATION_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}

