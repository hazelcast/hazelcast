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

import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.topic.TopicEvent;
import com.hazelcast.topic.TopicService;
import com.hazelcast.util.ExceptionUtil;

import java.util.List;

/**
 * User: sancar
 * Date: 2/26/13
 * Time: 11:44 AM
 */
abstract class TopicProxySupport extends AbstractDistributedObject<TopicService> implements InitializingObject {

    private final String name;

    TopicProxySupport(String name, NodeEngine nodeEngine, TopicService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    public void initialize() {
        final NodeEngine nodeEngine = getNodeEngine();
        TopicConfig config = nodeEngine.getConfig().getTopicConfig(name);
        final List<ListenerConfig> messageListenerConfigs = config.getMessageListenerConfigs();
        for (ListenerConfig listenerConfig : messageListenerConfigs) {
            MessageListener listener;
            try {
                listener = (MessageListener) listenerConfig.getImplementation();
                if (listener == null && listenerConfig.getClassName() != null) {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), listenerConfig.getClassName());
                }
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) listener).setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                addMessageListenerInternal(listener);
            }
        }
    }

    public LocalTopicStats getLocalTopicStatsInternal() {
        return getService().getLocalTopicStats(name);
    }

    public void publishInternal(Data message) {
        TopicEvent topicEvent = new TopicEvent(name, message, getNodeEngine().getLocalMember());
        getService().getLocalTopicStats(name).incrementPublishes();
        getService().publishEvent(name, topicEvent);
    }

    public String addMessageListenerInternal(MessageListener listener) {
        return getService().addMessageListener(name, listener);
    }

    public boolean removeMessageListenerInternal(final String registrationId) {
        return getService().removeMessageListener(name, registrationId);
    }

    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }
}
