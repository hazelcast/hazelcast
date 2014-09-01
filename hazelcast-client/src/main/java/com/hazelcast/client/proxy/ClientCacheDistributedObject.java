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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

public class ClientCacheDistributedObject
        extends ClientProxy {

    public ClientCacheDistributedObject(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    public ClientContext getClientContext() {
        return getContext();
    }

    @Override
    public <T> T toObject(Object data) {
        return super.toObject(data);
    }

    @Override
    public Data toData(Object o) {
        return super.toData(o);
    }

    @Override
    public <T> T invoke(ClientRequest req, Address address) {
        return super.invoke(req, address);
    }

    @Override
    public <T> T invoke(ClientRequest req) {
        return super.invoke(req);
    }

    @Override
    public <T> T invokeInterruptibly(ClientRequest req, Object key)
            throws InterruptedException {
        return super.invokeInterruptibly(req, key);
    }

    @Override
    public <T> T invoke(ClientRequest req, Object key) {
        return super.invoke(req, key);
    }
}
