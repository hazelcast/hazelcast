/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.topic.impl;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.MessageListener;

import javax.annotation.Nonnull;
import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Topic proxy used when global ordering is disabled (nodes get
 * the messages in the order that the messages are published).
 *
 * @param <E> the type of message in this topic
 */
public class TopicProxy<E> extends TopicProxySupport implements ITopic<E> {

    protected static final String NULL_MESSAGE_IS_NOT_ALLOWED = "Null message is not allowed!";
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    public TopicProxy(String name, NodeEngine nodeEngine, TopicService service) {
        super(name, nodeEngine, service);
    }

    @Override
    public void publish(@Nonnull E message) {
        checkNotNull(message, NULL_MESSAGE_IS_NOT_ALLOWED);
        publishInternal(message);
    }

    @Nonnull
    @Override
    public UUID addMessageListener(@Nonnull MessageListener<E> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        return addMessageListenerInternal(listener);
    }

    @Override
    public boolean removeMessageListener(@Nonnull UUID registrationId) {
        return removeMessageListenerInternal(registrationId);
    }

    @Nonnull
    @Override
    public LocalTopicStats getLocalTopicStats() {
        return getLocalTopicStatsInternal();
    }
}


