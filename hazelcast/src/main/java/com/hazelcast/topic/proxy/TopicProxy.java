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

package com.hazelcast.topic.proxy;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.topic.TopicService;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 2:06 PM
 */
public class TopicProxy<E> extends TopicProxySupport implements ITopic<E> {

    public TopicProxy(String name, NodeEngine nodeEngine, TopicService service) {
        super(name, nodeEngine, service);
    }

    public void publish(E message) {
        publishInternal(getNodeEngine().toData(message));
    }

    public String addMessageListener(MessageListener<E> listener) {
        return addMessageListenerInternal(listener);
    }

    public boolean removeMessageListener(final String registrationId) {
        return removeMessageListenerInternal(registrationId);
    }

    public LocalTopicStats getLocalTopicStats() {
        return getLocalTopicStatsInternal();
    }

}


