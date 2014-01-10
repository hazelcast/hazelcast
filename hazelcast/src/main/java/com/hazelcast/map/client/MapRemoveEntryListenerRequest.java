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

package com.hazelcast.map.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @author ali 23/12/13
 */
public class MapRemoveEntryListenerRequest extends CallableClientRequest {

    String name;

    String registrationId;

    public MapRemoveEntryListenerRequest() {
    }

    public MapRemoveEntryListenerRequest(String name, String registrationId) {
        this.name = name;
        this.registrationId = registrationId;
    }

    public Object call() throws Exception {
        final MapService service = getService();
        return service.removeEventListener(name, registrationId);
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.REMOVE_ENTRY_LISTENER;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("r", registrationId);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        registrationId = reader.readUTF("r");
    }
}
