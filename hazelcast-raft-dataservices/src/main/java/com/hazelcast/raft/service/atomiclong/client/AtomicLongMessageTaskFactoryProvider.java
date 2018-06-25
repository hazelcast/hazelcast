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

package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * Provider for Raft-based atomic long client message task factories
 */
public class AtomicLongMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int DESTROY_TYPE = 10000;
    public static final int ADD_AND_GET_TYPE = 10001;
    public static final int GET_AND_ADD_TYPE = 10002;
    public static final int GET_AND_SET_TYPE = 10003;
    public static final int COMPARE_AND_SET_TYPE = 10004;

    private final Node node;

    public AtomicLongMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[DESTROY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroyAtomicLongMessageTask(clientMessage, node, connection);
            }
        };

        factories[ADD_AND_GET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddAndGetMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_AND_ADD_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAndAddMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_AND_SET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAndSetMessageTask(clientMessage, node, connection);
            }
        };

        factories[COMPARE_AND_SET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CompareAndSetMessageTask(clientMessage, node, connection);
            }
        };

        return factories;
    }
}
