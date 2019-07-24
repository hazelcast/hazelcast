/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec;
import com.hazelcast.client.impl.protocol.codec.TopicPublishCodec;
import com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.topic.impl.DataAwareMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.client.impl.proxy.ClientMapProxy.NULL_LISTENER_IS_NOT_ALLOWED;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link ITopic}.
 *
 * @param <E> message type
 */
public class ClientTopicProxy<E> extends PartitionSpecificClientProxy implements ITopic<E> {

    public ClientTopicProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public void publish(@Nullable E message) {
        Data data = toData(message);
        ClientMessage request = TopicPublishCodec.encodeRequest(name, data);
        invokeOnPartition(request);
    }

    @Nonnull
    @Override
    public String addMessageListener(@Nonnull final MessageListener<E> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        EventHandler<ClientMessage> handler = new TopicItemHandler(listener);
        return registerListener(new Codec(), handler);
    }

    @Override
    public boolean removeMessageListener(@Nonnull String registrationId) {
        return deregisterListener(registrationId);
    }

    @Nonnull
    @Override
    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String toString() {
        return "ITopic{" + "name='" + name + '\'' + '}';
    }

    private final class TopicItemHandler extends TopicAddMessageListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {
        private final MessageListener<E> listener;

        private TopicItemHandler(MessageListener<E> listener) {
            this.listener = listener;
        }

        @Override
        public void handleTopicEvent(Data item, long publishTime, String uuid) {
            Member member = getContext().getClusterService().getMember(uuid);
            Message message = new DataAwareMessage(name, item, publishTime, member, getSerializationService());
            listener.onMessage(message);
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    private class Codec implements ListenerMessageCodec {

        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return TopicAddMessageListenerCodec.encodeRequest(name, localOnly);
        }

        @Override
        public String decodeAddResponse(ClientMessage clientMessage) {
            return TopicAddMessageListenerCodec.decodeResponse(clientMessage).response;
        }

        @Override
        public ClientMessage encodeRemoveRequest(String realRegistrationId) {
            return TopicRemoveMessageListenerCodec.encodeRequest(name, realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return TopicRemoveMessageListenerCodec.decodeResponse(clientMessage).response;
        }
    }
}
