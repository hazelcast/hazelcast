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

package com.hazelcast.raft.service.countdownlatch.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * Provider for Raft-based count down latch client message task factories
 */
public class CountDownLatchMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int DESTROY_TYPE = 13000;
    public static final int AWAIT_TYPE = 13001;
    public static final int COUNT_DOWN_TYPE = 13002;
    public static final int GET_REMAINING_COUNT_TYPE = 13003;
    public static final int GET_ROUND_TYPE = 13004;
    public static final int TRY_SET_COUNT_TYPE = 13005;

    private final Node node;

    public CountDownLatchMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[DESTROY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroyCountDownLatchMessageTask(clientMessage, node, connection);
            }
        };

        factories[AWAIT_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AwaitMessageTask(clientMessage, node, connection);
            }
        };

        factories[COUNT_DOWN_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CountDownMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_REMAINING_COUNT_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetRemainingCountMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_ROUND_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetRoundMessageTask(clientMessage, node, connection);
            }
        };

        factories[TRY_SET_COUNT_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TrySetCountMessageTask(clientMessage, node, connection);
            }
        };

        return factories;
    }
}
