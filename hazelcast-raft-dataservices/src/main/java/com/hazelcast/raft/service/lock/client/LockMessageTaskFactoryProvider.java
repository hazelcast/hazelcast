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

package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * Provider for Raft-based lock client message task factories
 */
public class LockMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int DESTROY_TYPE = 12000;
    public static final int LOCK_TYPE = 12001;
    public static final int TRY_LOCK_TYPE = 12002;
    public static final int UNLOCK_TYPE = 12003;
    public static final int FORCE_UNLOCK_TYPE = 12004;
    public static final int LOCK_OWNERSHIP_STATE = 12005;

    private final Node node;

    public LockMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[DESTROY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroyLockMessageTask(clientMessage, node, connection);
            }
        };

        factories[LOCK_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LockMessageTask(clientMessage, node, connection);
            }
        };

        factories[UNLOCK_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new UnlockMessageTask(clientMessage, node, connection);
            }
        };

        factories[FORCE_UNLOCK_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ForceUnlockMessageTask(clientMessage, node, connection);
            }
        };

        factories[TRY_LOCK_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TryLockMessageTask(clientMessage, node, connection);
            }
        };

        factories[LOCK_OWNERSHIP_STATE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetLockOwnershipStateMessageTask(clientMessage, node, connection);
            }
        };

        return factories;
    }
}
