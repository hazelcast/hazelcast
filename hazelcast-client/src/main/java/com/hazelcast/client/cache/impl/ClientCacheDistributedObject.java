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

package com.hazelcast.client.cache.impl;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;

/**
 * Basic client proxy object which serves as an accessor to {@link ClientContext}.
 *<p>
 * <b>Warning: DO NOT use this distributed object directly, instead use {@link ClientCacheProxy} through
 * {@link javax.cache.CacheManager}.</b>
 *</p>
 */
public class ClientCacheDistributedObject
        extends ClientProxy {

    public ClientCacheDistributedObject(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    public ClientContext getClientContext() {
        return getContext();
    }
}
