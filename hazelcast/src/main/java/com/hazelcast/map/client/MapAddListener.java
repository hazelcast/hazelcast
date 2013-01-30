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

package com.hazelcast.map.client;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.instance.Node;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.List;

public class MapAddListener extends MapCommandHandler {
    public MapAddListener(MapService mapService) {
        super(mapService);
    }

    @Override
    public Protocol processCall(final Node node, final Protocol protocol) {
        String name = protocol.args[0];
        System.out.println("Received the addListener for " + name);
        boolean includeValue = Boolean.valueOf(protocol.args[1]);
        final Data key = protocol.buffers.length > 0 ? protocol.buffers[0] : null;
        final DataMapProxy dataMapProxy = mapService.createDistributedObjectForClient(name);
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
                System.out.println("Sending the event");
                if (connection.live()) {
                    String[] args = new String[]{"map", dataMapProxy.getName(), entryEvent.getEventType().toString(),
                            entryEvent.getMember().getInetSocketAddress().getHostName() + ":" + entryEvent.getMember().getInetSocketAddress().getPort()};
                    List<Data> list = new ArrayList<Data>();
                    list.add(node.serializationService.toData(entryEvent.getKey()));
                    if (entryEvent.getValue() != null)
                        list.add(node.serializationService.toData(entryEvent.getValue()));
                    if (entryEvent.getOldValue() != null)
                        list.add(node.serializationService.toData(entryEvent.getOldValue()));
                    Protocol event = new Protocol(connection, Command.EVENT, args, list.toArray(new Data[]{}));
                    System.out.println("Connection is " + connection);
                    sendResponse(node, event, connection);
                } else {
                    System.out.println("on Server removing the listener");
                    dataMapProxy.removeEntryListener(this, key);
                }
            }
        };
        if (key == null)
            dataMapProxy.addEntryListener(entryListener, includeValue);
        else
            dataMapProxy.addEntryListener(entryListener, key, includeValue);
        return null;
    }
}
