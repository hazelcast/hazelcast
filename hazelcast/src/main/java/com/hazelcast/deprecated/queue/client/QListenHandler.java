/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.deprecated.queue.client;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.Node;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.queue.proxy.DataQueueProxy;

public class QListenHandler extends QueueCommandHandler {
    public QListenHandler(QueueService queueService) {
        super(queueService);
    }

    @Override
    public Protocol processCall(final Node node, final Protocol protocol) {
        String name = protocol.args[0];
        boolean includeValue = Boolean.valueOf(protocol.args[1]);
        final DataQueueProxy queue = qService.createDistributedObjectForClient(name);
        final TcpIpConnection connection = protocol.conn;
        ItemListener<Data> listener = new ItemListener<Data>() {
            public void itemAdded(ItemEvent<Data> item) {
                sendEvent(item);
            }

            public void itemRemoved(ItemEvent<Data> item) {
                sendEvent(item);
            }

            public void sendEvent(ItemEvent<Data> itemEvent) {
                System.out.println("Sending the event");
                if (connection.live()) {
                    String[] args = new String[]{queue.getName(), itemEvent.getEventType().toString(),
                            itemEvent.getMember().getInetSocketAddress().getHostName() + ":" + itemEvent.getMember().getInetSocketAddress().getPort()};
                    
                    Protocol event = new Protocol(connection, Command.EVENT, args, node.serializationService.toData(itemEvent.getItem()));
                    System.out.println("Connection is " + connection);
                    sendResponse(node, event, connection);
                } else {
                    System.out.println("on Server removing the listener");
//                    queue.removeItemListener(this);
                }
            }
        };

        queue.addItemListener(listener, includeValue);
        return null;
    }
}
