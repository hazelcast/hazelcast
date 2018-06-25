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

package com.hazelcast.raft.service.atomicref.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * Provider for Raft-based atomic reference client message task factories
 */
public class AtomicRefMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int DESTROY_TYPE = 11000;
    public static final int APPLY_TYPE = 11001;
    public static final int COMPARE_AND_SET_TYPE = 11002;
    public static final int CONTAINS_TYPE = 11003;
    public static final int GET_TYPE = 11004;
    public static final int SET_TYPE = 11005;

    private final Node node;

    public AtomicRefMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[DESTROY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroyAtomicRefMessageTask(clientMessage, node, connection);
            }
        };

        factories[APPLY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ApplyMessageTask(clientMessage, node, connection);
            }
        };

        factories[COMPARE_AND_SET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CompareAndSetMessageTask(clientMessage, node, connection);
            }
        };

        factories[CONTAINS_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ContainsMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetMessageTask(clientMessage, node, connection);
            }
        };

        factories[SET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new SetMessageTask(clientMessage, node, connection);
            }
        };

        return factories;
    }
}
