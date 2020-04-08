/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.function.Consumer;

/**
 * Base SQL service implementation which bridges optimizer implementation, public and private APIs.
 */
public class SqlServiceProxy implements Consumer<Packet> {

    private final NodeServiceProvider nodeServiceProvider;

    private volatile SqlInternalService internalService;

    public SqlServiceProxy(NodeEngineImpl nodeEngine) {
        // These two parameters will be taken from the config, when public API is ready.
        int operationThreadCount = Runtime.getRuntime().availableProcessors();
        int fragmentThreadCount = Runtime.getRuntime().availableProcessors();

        nodeServiceProvider = new NodeServiceProviderImpl(nodeEngine);

        String instanceName = nodeEngine.getHazelcastInstance().getName();
        InternalSerializationService serializationService = (InternalSerializationService) nodeEngine.getSerializationService();

        internalService = new SqlInternalService(
            instanceName,
            nodeServiceProvider,
            serializationService,
            operationThreadCount,
            fragmentThreadCount
        );
    }

    public void start() {
        internalService.start();
    }

    public void reset() {
        internalService.reset();
    }

    public void shutdown() {
        internalService.shutdown();
    }

    public SqlInternalService getInternalService() {
        return internalService;
    }

    /**
     * For testing only.
     */
    public void setInternalService(SqlInternalService internalService) {
        this.internalService = internalService;
    }

    @Override
    public void accept(Packet packet) {
        internalService.onPacket(packet);
    }
}
