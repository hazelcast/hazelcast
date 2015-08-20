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

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;

import java.security.Permission;

public class CacheRemovePartitionLostListenerRequest  extends BaseClientRemoveListenerRequest {


    public CacheRemovePartitionLostListenerRequest() {
    }

    public CacheRemovePartitionLostListenerRequest(String name, String registrationId) {
        super(name, registrationId);
    }

    public Object call() throws Exception {
        final ICacheService service = getService();
        return service.getNodeEngine().getEventService().
                deregisterListener(AbstractCacheService.SERVICE_NAME,
                        name, registrationId);
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.REMOVE_CACHE_PARTITION_LOST_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "removeCachePartitionLostListener";
    }
}
