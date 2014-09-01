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

package com.hazelcast.queue.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.DataAwareItemEvent;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.io.IOException;
import java.security.Permission;

/**
 * this class is used to attach a listener to targeted node which sends back the events to client for a queue
 */
public class AddListenerRequest extends CallableClientRequest implements SecureRequest, RetryableRequest {

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
        writer.writeUTF("n", name);
        writer.writeBoolean("i", includeValue);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        includeValue = reader.readBoolean("i");
    }

    @Override
    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final QueueService service = getService();

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
                if (endpoint.live()) {

                    if (!(event instanceof DataAwareItemEvent)) {
                        throw new IllegalArgumentException("Expecting: DataAwareItemEvent, Found: "
                                + event.getClass().getSimpleName());
                    }

                    DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) event;
                    Data item = dataAwareItemEvent.getItemData();
                    PortableItemEvent portableItemEvent = new PortableItemEvent(item, event.getEventType(),
                            event.getMember().getUuid());
                    endpoint.sendEvent(portableItemEvent, getCallId());
                }
            }
        };
        String registrationId = service.addItemListener(name, listener, includeValue);
        endpoint.setListenerRegistration(QueueService.SERVICE_NAME, name, registrationId);
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
