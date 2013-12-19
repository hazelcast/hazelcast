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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.topic.client.AddMessageListenerRequest;
import com.hazelcast.topic.client.PortableMessage;
import com.hazelcast.topic.client.PublishRequest;
import com.hazelcast.util.ExceptionUtil;

/**
 * @author ali 5/24/13
 */
public class ClientTopicProxy<E> extends ClientProxy implements ITopic<E> {

    private final String name;
    private volatile Data key;

    public ClientTopicProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    public void publish(E message) {
        final Data data = getContext().getSerializationService().toData(message);
        PublishRequest request = new PublishRequest(name, data);
        invoke(request);
    }

    public String addMessageListener(final MessageListener<E> listener) {
        AddMessageListenerRequest request = new AddMessageListenerRequest(name);
        EventHandler<PortableMessage> handler = new EventHandler<PortableMessage>() {
            public void handle(PortableMessage event) {
                E messageObject = (E) getContext().getSerializationService().toObject(event.getMessage());
                Member member = getContext().getClusterService().getMember(event.getUuid());
                Message<E> message = new Message<E>(name, messageObject, event.getPublishTime(), member);
                listener.onMessage(message);
            }
        };
        return listen(request, getKey(), handler);
    }

    public boolean removeMessageListener(String registrationId) {
        return stopListening(registrationId);
    }

    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    protected void onDestroy() {
    }

    private Data getKey() {
        if (key == null) {
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }

    private <T> T invoke(Object req) {
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
