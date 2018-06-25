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

package com.hazelcast.raft.service.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * Provider for Raft-based semaphore client message task factories
 */
public class SemaphoreMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int DESTROY_TYPE = 15000;
    public static final int ACQUIRE_PERMITS_TYPE = 15001;
    public static final int AVAILABLE_PERMITS_TYPE = 15002;
    public static final int CHANGE_PERMITS_TYPE = 15003;
    public static final int DRAIN_PERMITS_TYPE = 15004;
    public static final int INIT_SEMAPHORE_TYPE = 15005;
    public static final int RELEASE_PERMITS_TYPE = 15006;

    private final Node node;

    public SemaphoreMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[DESTROY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroySemaphoreMessageTask(clientMessage, node, connection);
            }
        };

        factories[ACQUIRE_PERMITS_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AcquirePermitsMessageTask(clientMessage, node, connection);
            }
        };

        factories[AVAILABLE_PERMITS_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AvailablePermitsMessageTask(clientMessage, node, connection);
            }
        };

        factories[CHANGE_PERMITS_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ChangePermitsMessageTask(clientMessage, node, connection);
            }
        };

        factories[DRAIN_PERMITS_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DrainPermitsMessageTask(clientMessage, node, connection);
            }
        };

        factories[INIT_SEMAPHORE_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new InitSemaphoreMessageTask(clientMessage, node, connection);
            }
        };

        factories[RELEASE_PERMITS_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ReleasePermitsMessageTask(clientMessage, node, connection);
            }
        };

        return factories;
    }
}
