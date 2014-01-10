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

package com.hazelcast.topic.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.topic.TopicPortableHook;
import com.hazelcast.topic.TopicService;

import java.io.IOException;

/**
 * @author ali 23/12/13
 */
public class RemoveMessageListenerRequest extends CallableClientRequest {

    String name;

    String registrationId;

    public RemoveMessageListenerRequest() {
    }

    public RemoveMessageListenerRequest(String name, String registrationId) {
        this.name = name;
        this.registrationId = registrationId;
    }

    public Object call() throws Exception {
        final TopicService service = getService();
        return service.removeMessageListener(name, registrationId);
    }

    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    public int getClassId() {
        return TopicPortableHook.REMOVE_LISTENER;
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
