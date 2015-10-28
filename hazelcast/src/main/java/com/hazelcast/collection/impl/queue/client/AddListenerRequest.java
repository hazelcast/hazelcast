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

package com.hazelcast.collection.impl.queue.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.collection.common.LazyDeserializingItemEvent;
import com.hazelcast.collection.impl.queue.QueuePortableHook;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.io.IOException;
import java.security.Permission;

/**
 * this class is used to attach a listener to targeted node which sends back the events to client for a queue
 */
public class AddListenerRequest extends BaseClientAddListenerRequest {

    private String name;
    private boolean includeValue;

    public AddListenerRequest() {
    }

    public AddListenerRequest(String name, boolean includeValue) {
        this.name = name;
        this.includeValue = includeValue;
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.ADD_LISTENER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        writer.writeBoolean("i", includeValue);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        includeValue = reader.readBoolean("i");
    }

    @Override
    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final QueueService service = getService();
        final Data partitionKey = serializationService.toData(name);
        ItemListener listener = new ItemListener() {
            @Override
            public void itemAdded(ItemEvent item) {
                send(item);
            }

            @Override
            public void itemRemoved(ItemEvent item) {
                send(item);
            }

            private void send(ItemEvent event) {
                if (endpoint.isAlive()) {

                    if (!(event instanceof LazyDeserializingItemEvent)) {
                        throw new IllegalArgumentException("Expecting: LazyDeserializingItemEvent, Found: "
                                + event.getClass().getSimpleName());
                    }

                    LazyDeserializingItemEvent itemEvent = (LazyDeserializingItemEvent) event;
                    Data item = itemEvent.getItemData();
                    PortableItemEvent portableItemEvent = new PortableItemEvent(item, event.getEventType(),
                            event.getMember().getUuid());
                    endpoint.sendEvent(partitionKey, portableItemEvent, getCallId());
                }
            }
        };
        String registrationId = service.addItemListener(name, listener, includeValue, localOnly);
        endpoint.addListenerDestroyAction(QueueService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "addItemListener";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, includeValue};
    }
}
