/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.io.IOException;

/**
 * @author ali 5/9/13
 */
public class AddListenerRequest extends CallableClientRequest implements Portable, InitializingObjectRequest {

    private String name;
    private boolean includeValue;

    public AddListenerRequest() {
    }

    public AddListenerRequest(String name, boolean includeValue) {
        this.name = name;
        this.includeValue = includeValue;
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public Object getObjectId() {
        return name;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.ADD_LISTENER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeBoolean("i",includeValue);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        includeValue = reader.readBoolean("i");
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final QueueService service = getService();

        ItemListener listener = new ItemListener() {
            public void itemAdded(ItemEvent item) {
                send(item);
            }

            public void itemRemoved(ItemEvent item) {
                send(item);
            }

            private void send(ItemEvent event){
                if (endpoint.live()){
                    Data item = clientEngine.toData(event.getItem());
                    PortableItemEvent portableItemEvent = new PortableItemEvent(item, event.getEventType(), event.getMember().getUuid());
                    clientEngine.sendResponse(endpoint, portableItemEvent);
                }
            }
        };
        String registrationId = service.addItemListener(name, listener, includeValue);
        endpoint.setListenerRegistration(QueueService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }
}
