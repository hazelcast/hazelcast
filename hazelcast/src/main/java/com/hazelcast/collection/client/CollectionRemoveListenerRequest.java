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

package com.hazelcast.collection.client;

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.EventService;

import java.io.IOException;
import java.security.Permission;

public class CollectionRemoveListenerRequest extends BaseClientRemoveListenerRequest {

    private String serviceName;

    public CollectionRemoveListenerRequest() {
    }

    public CollectionRemoveListenerRequest(String name, String registrationId, String serviceName) {
        super(name, registrationId);
        this.serviceName = serviceName;
    }

    public Object call() throws Exception {
        final ClientEngine clientEngine = getClientEngine();
        final EventService eventService = clientEngine.getEventService();
        return eventService.deregisterListener(serviceName, name, registrationId);
    }

    public String getServiceName() {
        return serviceName;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.COLLECTION_REMOVE_LISTENER;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("s", serviceName);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        serviceName = reader.readUTF("s");
    }

    @Override
    public Permission getRequiredPermission() {
        return ActionConstants.getPermission(name, serviceName, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "removeItemListener";
    }
}
