/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.TopicPublishAllCodec;
import com.hazelcast.client.impl.protocol.codec.TopicPublishCodec;
import com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.impl.DataAwareMessage;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link ITopic}.
 *
 * @param <E> message type
 */
public class ClientTopicProxy<E> extends PartitionSpecificClientProxy implements ITopic<E> {

    private static final String NULL_MESSAGE_IS_NOT_ALLOWED = "Null message is not allowed!";
    private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    public ClientTopicProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public void publish(@Nonnull E message) {
        checkNotNull(message, NULL_MESSAGE_IS_NOT_ALLOWED);
        Data data = toData(message);
        ClientMessage request = TopicPublishCodec.encodeRequest(name, data);
        invokeOnPartition(request);
    }

    @Override
    public InternalCompletableFuture<Void> publishAsync(@Nonnull E message) {
        checkNotNull(message, NULL_MESSAGE_IS_NOT_ALLOWED);

        Data data = toData(message);
        final ClientMessage request = TopicPublishCodec.encodeRequest(name, data);
        return invokeOnPartitionAsync(request, clientMessage -> null);
    }

    @Nonnull
    @Override
    public UUID addMessageListener(@Nonnull final MessageListener<E> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        EventHandler<ClientMessage> handler = new TopicItemHandler(listener);
        return registerListener(new Codec(), handler);
    }

    @Override
    public boolean removeMessageListener(@Nonnull UUID registrationId) {
        return deregisterListener(registrationId);
    }

    @Nonnull
    @Override
    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public void publishAll(@Nonnull Collection<? extends E> messages) {
        checkNotNull(messages, NULL_MESSAGE_IS_NOT_ALLOWED);
        checkNoNullInside(messages, NULL_MESSAGE_IS_NOT_ALLOWED);
        ClientMessage request = getClientMessage(messages);
        invokeOnPartition(request);
    }

    @Override
    public InternalCompletableFuture<Void> publishAllAsync(@Nonnull Collection<? extends E> messages) {
        checkNotNull(messages, NULL_MESSAGE_IS_NOT_ALLOWED);
        checkNoNullInside(messages, NULL_MESSAGE_IS_NOT_ALLOWED);
        final ClientMessage request = getClientMessage(messages);
        return invokeOnPartitionAsync(request, clientMessage -> null);
    }

    private ClientMessage getClientMessage(@Nonnull Collection<? extends E> messages) {
        Collection<Data> dataCollection = objectToDataCollection(messages, getSerializationService());
        return TopicPublishAllCodec.encodeRequest(name, dataCollection);
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
        public void handleTopicEvent(Data item, long publishTime, UUID uuid) {
            Member member = getContext().getClusterService().getMember(uuid);
            Message message = new DataAwareMessage(name, item, publishTime, member, getSerializationService());
            listener.onMessage(message);
        }
    }

    private class Codec implements ListenerMessageCodec {

        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return TopicAddMessageListenerCodec.encodeRequest(name, localOnly);
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return TopicAddMessageListenerCodec.decodeResponse(clientMessage);
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return TopicRemoveMessageListenerCodec.encodeRequest(name, realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return TopicRemoveMessageListenerCodec.decodeResponse(clientMessage);
        }
    }
}
