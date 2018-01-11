/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

public class IdGeneratorProxy
        extends AbstractDistributedObject<IdGeneratorService>
        implements IdGenerator {

    private final String name;

    private final IdGeneratorImpl idGeneratorImpl;


    public IdGeneratorProxy(IAtomicLong blockGenerator, String name, NodeEngine nodeEngine, IdGeneratorService service) {
        super(nodeEngine, service);
        this.name = name;
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
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return IdGeneratorService.SERVICE_NAME;
    }

    @Override
    protected void postDestroy() {
        idGeneratorImpl.destroy();
    }
}
