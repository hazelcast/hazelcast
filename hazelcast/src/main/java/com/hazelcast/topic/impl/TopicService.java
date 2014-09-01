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

package com.hazelcast.topic.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.topic.DataAwareMessage;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class TopicService implements ManagedService, RemoteService, EventPublishingService {

    public static final String SERVICE_NAME = "hz:impl:topicService";
    public static final int ORDERING_LOCKS_LENGTH = 1000;

    private final ConcurrentMap<String, LocalTopicStatsImpl> statsMap = new ConcurrentHashMap<String, LocalTopicStatsImpl>();
    private final Lock[] orderingLocks = new Lock[ORDERING_LOCKS_LENGTH];
    private NodeEngine nodeEngine;

    private final ConstructorFunction<String, LocalTopicStatsImpl> localTopicStatsConstructorFunction =
            new ConstructorFunction<String, LocalTopicStatsImpl>() {
                public LocalTopicStatsImpl createNew(String mapName) {
                    return new LocalTopicStatsImpl();
                }
            };
    private EventService eventService;
    private ILogger logger;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        for (int i = 0; i < orderingLocks.length; i++) {
            orderingLocks[i] = new ReentrantLock();
        }
        eventService = nodeEngine.getEventService();
        this.logger = nodeEngine.getLogger(TopicService.class);
    }

    //only for testing
    public ConcurrentMap<String, LocalTopicStatsImpl> getStatsMap() {
        return statsMap;
    }

    @Override
    public void reset() {
        statsMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    public Lock getOrderLock(String key) {
        int index = getOrderLockIndex(key);
        return orderingLocks[index];
    }

    private int getOrderLockIndex(String key) {
        int hash = key.hashCode();
        if (hash == Integer.MIN_VALUE) {
            return 0;
        } else {
            return Math.abs(hash) % orderingLocks.length;
        }
    }

    @Override
    public TopicProxy createDistributedObject(String name) {
        if (isGlobalOrderingEnabled(name)) {
            return new TotalOrderedTopicProxy(name, nodeEngine, this);
        } else {
            return new TopicProxy(name, nodeEngine, this);
        }
    }

    private boolean isGlobalOrderingEnabled(String name) {
        TopicConfig topicConfig = nodeEngine.getConfig().findTopicConfig(name);
        return topicConfig.isGlobalOrderingEnabled();
    }

    @Override
    public void destroyDistributedObject(String objectId) {
        statsMap.remove(objectId);
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        TopicEvent topicEvent = (TopicEvent) event;
        ClusterService clusterService = nodeEngine.getClusterService();
        MemberImpl member = clusterService.getMember(topicEvent.publisherAddress);
        if (member == null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Dropping message from unknown address:" + topicEvent.publisherAddress);
            }
            return;
        }
        Message message = new DataAwareMessage(topicEvent.name, topicEvent.data, topicEvent.publishTime, member
                , nodeEngine.getSerializationService());
        incrementReceivedMessages(topicEvent.name);
        MessageListener messageListener = (MessageListener) listener;
        messageListener.onMessage(message);
    }

    public LocalTopicStatsImpl getLocalTopicStats(String name) {
        return getOrPutSynchronized(statsMap, name, statsMap, localTopicStatsConstructorFunction);
    }

    public void incrementPublishes(String topicName) {
        getLocalTopicStats(topicName).incrementPublishes();
    }

    public void incrementReceivedMessages(String topicName) {
        getLocalTopicStats(topicName).incrementReceives();
    }

    public void publishEvent(String name, TopicEvent event) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(TopicService.SERVICE_NAME, name);
        eventService.publishEvent(TopicService.SERVICE_NAME, registrations, event, name.hashCode());
    }

    public String addMessageListener(String name, MessageListener listener) {
        EventRegistration eventRegistration = eventService.registerListener(TopicService.SERVICE_NAME, name, listener);
        return eventRegistration.getId();
    }

    public boolean removeMessageListener(String name, String registrationId) {
        return eventService.deregisterListener(TopicService.SERVICE_NAME, name, registrationId);
    }
}
