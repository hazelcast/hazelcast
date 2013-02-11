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

package com.hazelcast.collection.client;

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.core.ICollection;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.protocol.Command;

public abstract class CollectionItemListenHandler extends ClientCommandHandler {
    final CollectionService collectionService;
    public CollectionItemListenHandler(CollectionService collectionService) {
        this.collectionService = collectionService;
    }

    @Override
    public Protocol processCall(final Node node, Protocol protocol) {
        String name = protocol.args[0];
        CollectionProxyId id = getCollectionProxyId(protocol);
        final ICollection proxy = (ICollection) collectionService.createDistributedObjectForClient(id);
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
                System.out.println("Sending event " + itemEvent);
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

    protected abstract CollectionProxyId getCollectionProxyId(Protocol protocol);
}
