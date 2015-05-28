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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec;
import com.hazelcast.client.impl.protocol.codec.TopicPublishCodec;
import com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
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

public class ClientTopicProxy<E> extends ClientProxy implements ITopic<E> {

    private final String name;
    private volatile Data key;

    public ClientTopicProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public void publish(E message) {
        SerializationService serializationService = getContext().getSerializationService();
        Data data = serializationService.toData(message);
        ClientMessage request = TopicPublishCodec.encodeRequest(name, data);
        invoke(request);
    }

    @Override
    public String addMessageListener(final MessageListener<E> listener) {
        ClientMessage request = TopicAddMessageListenerCodec.encodeRequest(name);

        EventHandler<ClientMessage> handler = new TopicItemHandler(listener);
        return listen(request, getKey(), handler);
    }

    @Override
    public boolean removeMessageListener(String registrationId) {
        ClientMessage request = TopicRemoveMessageListenerCodec.encodeRequest(name, registrationId);
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
    protected <T> T invoke(ClientMessage clientMessage) {
        return super.invoke(clientMessage, getKey());
    }

    @Override
    public String toString() {
        return "ITopic{" + "name='" + getName() + '\'' + '}';
    }

    private final class TopicItemHandler extends TopicAddMessageListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {
        private final MessageListener<E> listener;

        private TopicItemHandler(MessageListener<E> listener) {
            this.listener = listener;
        }

        @Override
        public void handle(Data item, long publishTime, String uuid) {
            final SerializationService serializationService = getContext().getSerializationService();
            final ClientClusterService clusterService = getContext().getClusterService();

            E messageObject = serializationService.toObject(item);
            Member member = clusterService.getMember(uuid);
            Message<E> message = new Message<E>(name, messageObject, publishTime, member);
            listener.onMessage(message);
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }
    }
}
