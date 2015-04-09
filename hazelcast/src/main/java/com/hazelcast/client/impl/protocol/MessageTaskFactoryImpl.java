/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask;
import com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask;
import com.hazelcast.client.impl.protocol.task.MapAddEntryListenerTask;
import com.hazelcast.client.impl.protocol.task.MapPutMessageTask;
import com.hazelcast.client.impl.protocol.task.NoSuchMessageTask;
import com.hazelcast.client.impl.protocol.task.RegisterMembershipListenerMessageTask;
import com.hazelcast.client.impl.protocol.util.Int2ObjectHashMap;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

/**
 * Message task factory
 */
public class MessageTaskFactoryImpl {

    private final Int2ObjectHashMap<MessageTaskFactory> tasks = new Int2ObjectHashMap<MessageTaskFactory>();

    private final Node node;

    public MessageTaskFactoryImpl(Node node) {
        this.node = node;
        initFactories();
    }

    public void initFactories() {
        tasks.put(ClientMessageType.AUTHENTICATION_DEFAULT_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new AuthenticationMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.AUTHENTICATION_CUSTOM_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.MAP_PUT_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new MapPutMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.ADD_ENTRY_LISTENER_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new MapAddEntryListenerTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.REGISTER_MEMBERSHIP_LISTENER_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new RegisterMembershipListenerMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.CREATE_PROXY_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new CreateProxyMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.GET_PARTITIONS_REQUEST.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new GetPartitionsMessageTask(clientMessage, node, connection);
            }
        });

        //TODO more factories to come here
    }

    public PartitionSpecificRunnable createMessageTask(ClientMessage clientMessage, Connection connection) {
        final MessageTaskFactory factory = tasks.get(clientMessage.getMessageType());
        if (factory != null) {
            return factory.create(clientMessage, node, connection);
        }
        return new NoSuchMessageTask(clientMessage, node, connection);
    }

}
