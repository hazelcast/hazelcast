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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Properties;

/**
 * @ali 5/28/13
 */
public class IdGeneratorService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:idGeneratorService";

    public static final String ATOMIC_LONG_NAME = "hz:atomic:idGenerator:";

    private NodeEngine nodeEngine;

    public IdGeneratorService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void reset() {
    }

    public void shutdown() {
    }

    public IAtomicLong getAtomicLong(String name){
        return nodeEngine.getHazelcastInstance().getAtomicLong(ATOMIC_LONG_NAME+name);
    }

    public DistributedObject createDistributedObject(Object objectId) {
        String name = String.valueOf(objectId);
        return new IdGeneratorProxy(getAtomicLong(name), name);
    }

    public void destroyDistributedObject(Object objectId) {
    }
}
