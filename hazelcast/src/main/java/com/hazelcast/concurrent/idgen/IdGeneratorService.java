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

package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Properties;

public class IdGeneratorService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:idGeneratorService";
    public static final String ATOMIC_LONG_NAME = "hz:atomic:idGenerator:";

    private NodeEngine nodeEngine;

    public IdGeneratorService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    private IAtomicLong getBlockGenerator(String name) {
        HazelcastInstance hazelcastInstance = nodeEngine.getHazelcastInstance();
        return hazelcastInstance.getAtomicLong(ATOMIC_LONG_NAME + name);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        IAtomicLong blockGenerator = getBlockGenerator(name);
        return new IdGeneratorProxy(blockGenerator, name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
    }
}
