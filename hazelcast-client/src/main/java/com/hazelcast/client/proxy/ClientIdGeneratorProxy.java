/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.idgen.IdGeneratorImpl;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientIdGeneratorProxy extends ClientProxy implements IdGenerator {

    private final IdGeneratorImpl idGeneratorImpl;


    public ClientIdGeneratorProxy(String serviceName, String objectId, IAtomicLong blockGenerator) {
        super(serviceName, objectId);
        this.idGeneratorImpl = new IdGeneratorImpl(blockGenerator);
    }

    public boolean init(long id) {
        return idGeneratorImpl.init(id);
    }

    public long newId() {
        return idGeneratorImpl.newId();
    }

    protected void onDestroy() {
        idGeneratorImpl.destroy();
    }

    @Override
    public String toString() {
        return "IdGenerator{" + "name='" + name + '\'' + '}';
    }
}
