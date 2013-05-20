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

package com.hazelcast.client.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

/**
 * @mdogan 5/16/13
 */
public abstract class ClientProxy implements DistributedObject {

    private final String serviceName;

    private final Object objectId;

    private volatile ClientContext context;

    protected ClientProxy(String serviceName, Object objectId) {
        this.serviceName = serviceName;
        this.objectId = objectId;
    }

    public final ClientContext getContext() {
        final ClientContext ctx = context;
        if (ctx == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return context;
    }

    final void setContext(ClientContext context) {
        this.context = context;
    }

    public final Object getId() {
        return objectId;
    }

    public final String getServiceName() {
        return serviceName;
    }

    public final void destroy() {
        onDestroy();
        context = null;
    }

    protected abstract void onDestroy();
}
