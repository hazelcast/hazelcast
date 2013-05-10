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

package com.hazelcast.deprecated.collection.set.client;

import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.Node;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.deprecated.nio.protocol.Command;

public class SetListenHandler extends SetCommandHandler {
    public SetListenHandler(CollectionService collectionService) {
        super(collectionService);
    }

    @Override
    public Protocol processCall(final Node node, Protocol protocol) {
        String name = protocol.args[0];
        final ObjectSetProxy proxy = (ObjectSetProxy) collectionService.createDistributedObjectForClient(name);
        boolean includeValue = Boolean.valueOf(protocol.args[1]);
        final TcpIpConnection connection = protocol.conn;
        proxy.addItemListener(new ItemListener<Object>() {
            public void itemAdded(ItemEvent<Object> item) {
                sendEvent(item);
            }

            public void itemRemoved(ItemEvent<Object> item) {
                sendEvent(item);
            }

            public void sendEvent(ItemEvent<Object> itemEvent) {
                if (connection.live()) {
                    String[] args = new String[]{proxy.getName(), itemEvent.getEventType().toString(),
                            itemEvent.getMember().getInetSocketAddress().getHostName() + ":" + itemEvent.getMember().getInetSocketAddress().getPort()};
                    Protocol event = new Protocol(connection, Command.EVENT, args, node.serializationService.toData(itemEvent.getItem()));
                    sendResponse(node, event, connection);
                } else {
                    proxy.removeItemListener(this);
                }
            }
        }, includeValue);
        return null;
    }

    @Override
    protected Protocol processCall(final ObjectSetProxy proxy, Protocol protocol) {
        throw new RuntimeException("Never should reach here!");
    }
}
