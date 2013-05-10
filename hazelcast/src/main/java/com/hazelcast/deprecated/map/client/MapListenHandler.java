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

package com.hazelcast.deprecated.map.client;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.instance.Node;
import com.hazelcast.map.DataAwareEntryEvent;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.List;

public class MapListenHandler extends MapCommandHandler {
    public MapListenHandler(MapService mapService) {
        super(mapService);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Protocol processCall(final Node node, final Protocol protocol) {
        String name = protocol.args[0];
        System.out.println("Received the addListener for " + name);
        boolean includeValue = Boolean.valueOf(protocol.args[1]);
        final Data key = protocol.hasBuffer() ? protocol.buffers[0] : null;
        final DataMapProxy dataMapProxy = mapService.createDistributedObjectForClient(name);
        final TcpIpConnection connection = protocol.conn;
        EntryListener<Data, Data> entryListener = new EntryListener() {
            public void entryAdded(EntryEvent entryEvent) {
                sendEvent(entryEvent);
            }

            public void entryRemoved(EntryEvent entryEvent) {
                sendEvent(entryEvent);
            }

            public void entryUpdated(EntryEvent entryEvent) {
                sendEvent(entryEvent);
            }

            public void entryEvicted(EntryEvent entryEvent) {
                sendEvent(entryEvent);
            }

            public void sendEvent(EntryEvent entryEvent) {
                System.out.println("Sending the event");

                if (connection.live()) {
                    DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) entryEvent;
                    String[] args = new String[]{"map", dataMapProxy.getName(), entryEvent.getEventType().toString(),
                            entryEvent.getMember().getInetSocketAddress().getHostName() + ":" + entryEvent.getMember().getInetSocketAddress().getPort()};
                    List<Data> list = new ArrayList<Data>();
                    list.add(dataAwareEntryEvent.getKeyData());
                    if (dataAwareEntryEvent.getOldValueData() != null)
                        list.add(dataAwareEntryEvent.getNewValueData());
                    if (dataAwareEntryEvent.getOldValueData() != null)
                        list.add(dataAwareEntryEvent.getOldValueData());
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
