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

package com.hazelcast.cache.impl;

import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

/**
 * Basic distributed object which serves as an accessor to {@link CacheService} and {@link NodeEngine}.
 *<p>
 * <b>Warning: DO NOT use this distributed object directly, instead use {@link CacheProxy} through
 * {@link javax.cache.CacheManager}.</b>
 *</p>
 */
public class CacheDistributedObject
        extends AbstractDistributedObject<ICacheService> {

    private String name;
    private boolean isDestroy;

    protected CacheDistributedObject(String name, NodeEngine nodeEngine, ICacheService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    protected void postDestroy() {
        isDestroy = true;
    }

    public boolean isDestroy() {
        return isDestroy;
    }
}
