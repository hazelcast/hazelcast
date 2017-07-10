/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

public abstract class TopicProxySupport extends AbstractDistributedObject<TopicService> implements InitializingObject {

    private final String name;
    private final ClassLoader configClassLoader;
    private final TopicService topicService;
    private final LocalTopicStatsImpl topicStats;
    private boolean multithreaded;

    public TopicProxySupport(String name, NodeEngine nodeEngine, TopicService service) {
        super(nodeEngine, service);
        this.name = name;
        this.configClassLoader = nodeEngine.getConfigClassLoader();
        this.topicService = service;
        this.topicStats = topicService.getLocalTopicStats(name);
    }

    @Override
    public void initialize() {
        NodeEngine nodeEngine = getNodeEngine();
        TopicConfig config = nodeEngine.getConfig().findTopicConfig(name);
        multithreaded = config.isMultiThreadingEnabled();
        for (ListenerConfig listenerConfig : config.getMessageListenerConfigs()) {
            initialize(listenerConfig);
        }
    }

    private void initialize(ListenerConfig listenerConfig) {
        NodeEngine nodeEngine = getNodeEngine();

        MessageListener listener = loadListener(listenerConfig);

        if (listener == null) {
            return;
        }

        if (listener instanceof HazelcastInstanceAware) {
            HazelcastInstanceAware hazelcastInstanceAware = (HazelcastInstanceAware) listener;
            hazelcastInstanceAware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
        }
        addMessageListenerInternal(listener);
    }

    private MessageListener loadListener(ListenerConfig listenerConfig) {
        try {
            MessageListener listener = (MessageListener) listenerConfig.getImplementation();
            if (listener == null && listenerConfig.getClassName() != null) {
                listener = ClassLoaderUtil.newInstance(configClassLoader, listenerConfig.getClassName());
            }
            return listener;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public LocalTopicStats getLocalTopicStatsInternal() {
        return topicService.getLocalTopicStats(name);
    }

    /**
     * Publishes the message and increases the local statistics
     * for the number of published messages.
     *
     * @param message the message to be published
     */
    public void publishInternal(Object message) {
        topicStats.incrementPublishes();
        topicService.publishMessage(name, message, multithreaded);
    }

    public String addMessageListenerInternal(MessageListener listener) {
        return topicService.addMessageListener(name, listener, false);
    }

    public boolean removeMessageListenerInternal(final String registrationId) {
        return topicService.removeMessageListener(name, registrationId);
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
