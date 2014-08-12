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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.topic.impl.client.AddMessageListenerRequest;
import com.hazelcast.topic.impl.client.PortableMessage;
import com.hazelcast.topic.impl.client.PublishRequest;
import com.hazelcast.topic.impl.client.RemoveMessageListenerRequest;

public class ClientTopicProxy<E> extends ClientProxy implements ITopic<E> {

    private final String name;
    private volatile Data key;

    public ClientTopicProxy(String instanceName, String serviceName, String objectId) {
        super(instanceName, serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public void publish(E message) {
        SerializationService serializationService = getContext().getSerializationService();
        final Data data = serializationService.toData(message);
        PublishRequest request = new PublishRequest(name, data);
        invoke(request);
    }

    @Override
    public String addMessageListener(final MessageListener<E> listener) {
        AddMessageListenerRequest request = new AddMessageListenerRequest(name);

        EventHandler<PortableMessage> handler = new EventHandler<PortableMessage>() {
            @Override
            public void handle(PortableMessage event) {
                SerializationService serializationService = getContext().getSerializationService();
                ClientClusterService clusterService = getContext().getClusterService();

                E messageObject = serializationService.toObject(event.getMessage());
                Member member = clusterService.getMember(event.getUuid());
                Message<E> message = new Message<E>(name, messageObject, event.getPublishTime(), member);
                listener.onMessage(message);
            }

            @Override
            public void onListenerRegister() {

            }
        };
        return listen(request, getKey(), handler);
    }

    @Override
    public boolean removeMessageListener(String registrationId) {
        final RemoveMessageListenerRequest request = new RemoveMessageListenerRequest(name, registrationId);
        return stopListening(request, registrationId);
    }

    @Override
    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    private Data getKey() {
        if (key == null) {
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }

    @Override
    protected  <T> T invoke(ClientRequest req) {
        return super.invoke(req, getKey());
    }

    @Override
    public String toString() {
        return "ITopic{" + "name='" + getName() + '\'' + '}';
    }
}
