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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.cp.internal.datastructures.unsafe.idgen.IdGeneratorImpl;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.core.IdGenerator;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientIdGeneratorProxy extends ClientProxy implements IdGenerator {

    private final IdGeneratorImpl idGeneratorImpl;

    public ClientIdGeneratorProxy(String serviceName, String objectId, ClientContext context, IAtomicLong blockGenerator) {
        super(serviceName, objectId, context);
        this.idGeneratorImpl = new IdGeneratorImpl(blockGenerator);
    }

    @Override
    public boolean init(long id) {
        return idGeneratorImpl.init(id);
    }

    @Override
    public long newId() {
        return idGeneratorImpl.newId();
    }

    @Override
    protected void onDestroy() {
        idGeneratorImpl.destroy();
    }

    @Override
    public String toString() {
        return "IdGenerator{" + "name='" + name + '\'' + '}';
    }
}
