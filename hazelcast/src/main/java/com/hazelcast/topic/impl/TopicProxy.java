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

package com.hazelcast.topic.impl;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.spi.NodeEngine;

/**
 * Topic proxy used when global ordering is disabled (nodes get
 * the messages in the order that the messages are published).
 *
 * @param <E> the type of message in this topic
 */
public class TopicProxy<E> extends TopicProxySupport implements ITopic<E> {

    public TopicProxy(String name, NodeEngine nodeEngine, TopicService service) {
        super(name, nodeEngine, service);
    }

    @Override
    public void publish(E message) {
        publishInternal(message);
    }

    @Override
    public String addMessageListener(MessageListener<E> listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        return addMessageListenerInternal(listener);
    }

    @Override
    public boolean removeMessageListener(String registrationId) {
        return removeMessageListenerInternal(registrationId);
    }

    @Override
    public LocalTopicStats getLocalTopicStats() {
        return getLocalTopicStatsInternal();
    }
}


