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

package com.hazelcast.topic.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.client.CallableClientRequest;
import com.hazelcast.client.client.RetryableRequest;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.topic.impl.TopicPortableHook;
import com.hazelcast.topic.impl.TopicService;

import java.io.IOException;
import java.security.Permission;

public class AddMessageListenerRequest extends CallableClientRequest implements RetryableRequest {

    private String name;

    public AddMessageListenerRequest() {
    }

    public AddMessageListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public String call() throws Exception {
        TopicService service = getService();
        ClientEndpoint endpoint = getEndpoint();
        MessageListener listener = new MessageListenerImpl(endpoint, serializationService, getCallId());
        String registrationId = service.addMessageListener(name, listener);
        endpoint.setListenerRegistration(TopicService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicPortableHook.ADD_LISTENER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(name, ActionConstants.ACTION_LISTEN);
    }

    private static class MessageListenerImpl implements MessageListener {
        private final ClientEndpoint endpoint;
        private final SerializationService serializationService;
        private final int callId;

        public MessageListenerImpl(ClientEndpoint endpoint, SerializationService serializationService, int callId) {
            this.endpoint = endpoint;
            this.serializationService = serializationService;
            this.callId = callId;
        }

        @Override
        public void onMessage(Message message) {
            if (!endpoint.live()) {
                return;
            }

            Data messageData = serializationService.toData(message.getMessageObject());
            String publisherUuid = message.getPublishingMember().getUuid();
            PortableMessage portableMessage = new PortableMessage(messageData, message.getPublishTime(), publisherUuid);
            endpoint.sendEvent(portableMessage, callId);
        }
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "addMessageListener";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null};
    }
}
