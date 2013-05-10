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

package com.hazelcast.deprecated.collection.multimap.client;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.multimap.ObjectMultiMapProxy;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.instance.Node;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.List;

public class ListenHandler extends MultiMapCommandHandler {
    public ListenHandler(CollectionService collectionService) {
        super(collectionService);
    }

    @Override
    public Protocol processCall(final Node node, final Protocol protocol) {
        String name = protocol.args[0];
        boolean includeValue = Boolean.valueOf(protocol.args[1]);
        final Data key = protocol.hasBuffer() ? protocol.buffers[0] : null;
        CollectionProxyId id = new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP);
        final ObjectMultiMapProxy proxy = (ObjectMultiMapProxy) collectionService.createDistributedObjectForClient(id);
        final TcpIpConnection connection = protocol.conn;
        EntryListener<Data, Data> entryListener = new EntryListener<Data, Data>() {
            public void entryAdded(EntryEvent<Data, Data> entryEvent) {
                sendEvent(entryEvent);
            }

            public void entryRemoved(EntryEvent<Data, Data> entryEvent) {
                sendEvent(entryEvent);
            }

            public void entryUpdated(EntryEvent<Data, Data> entryEvent) {
                sendEvent(entryEvent);
            }

            public void entryEvicted(EntryEvent<Data, Data> entryEvent) {
                sendEvent(entryEvent);
            }

            public void sendEvent(EntryEvent<Data, Data> entryEvent) {
                System.out.println("Here is the event " +entryEvent);
                if (connection.live()) {
                    String[] args = new String[]{"map", proxy.getName(), entryEvent.getEventType().toString(),
                            entryEvent.getMember().getInetSocketAddress().getHostName() + ":" + entryEvent.getMember().getInetSocketAddress().getPort()};
                    List<Data> list = new ArrayList<Data>();
                    list.add(node.serializationService.toData(entryEvent.getKey()));
                    if (entryEvent.getValue() != null)
                        list.add(node.serializationService.toData(entryEvent.getValue()));
                    if (entryEvent.getOldValue() != null)
                        list.add(node.serializationService.toData(entryEvent.getOldValue()));
                    Protocol event = new Protocol(connection, Command.EVENT, args, list.toArray(new Data[]{}));
                    sendResponse(node, event, connection);
                } else {
                    proxy.removeEntryListener(this, key);
                }
            }
        };
        System.out.println("Name is " + name);
        if (key == null)
            proxy.addEntryListener(entryListener, includeValue);
        else
            proxy.addEntryListener(entryListener, key, includeValue);
        return null;
    }

    @Override
    protected Protocol processCall(ObjectMultiMapProxy proxy, Protocol protocol) {
        return null;
    }
}
