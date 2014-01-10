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

import com.hazelcast.client.*;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.topic.TopicPortableHook;
import com.hazelcast.topic.TopicService;

import java.io.IOException;
import java.security.Permission;

/**
 * @author ali 5/24/13
 */
public class AddMessageListenerRequest extends CallableClientRequest implements Portable, SecureRequest, RetryableRequest {

    private String name;

    public AddMessageListenerRequest() {
    }

    public AddMessageListenerRequest(String name) {
        this.name = name;
    }

    public Object call() throws Exception {
        final TopicService service = getService();
        final ClientEngine clientEngine = getClientEngine();
        final ClientEndpoint endpoint = getEndpoint();
        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
                if (endpoint.live()){
                    Data messageData = clientEngine.toData(message.getMessageObject());
                    PortableMessage portableMessage = new PortableMessage(messageData, message.getPublishTime(), message.getPublishingMember().getUuid());
                    endpoint.sendEvent(portableMessage, getCallId());
                }
            }
        };
        String registrationId = service.addMessageListener(name, listener);
        endpoint.setListenerRegistration(TopicService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    public int getClassId() {
        return TopicPortableHook.ADD_LISTENER;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    public Permission getRequiredPermission() {
        return new TopicPermission(name, ActionConstants.ACTION_LISTEN);
    }
}
