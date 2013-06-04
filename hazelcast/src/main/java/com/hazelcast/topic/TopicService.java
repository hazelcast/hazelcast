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

package com.hazelcast.topic;

import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.spi.*;
import com.hazelcast.topic.proxy.TopicProxy;
import com.hazelcast.topic.proxy.TotalOrderedTopicProxy;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 1:50 PM
 */
public class TopicService implements ManagedService, RemoteService, EventPublishingService {

    public static final String SERVICE_NAME = "hz:impl:topicService";
    private final Lock[] orderingLocks = new Lock[1000];
    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, LocalTopicStatsImpl> statsMap = new ConcurrentHashMap<String, LocalTopicStatsImpl>();

    private final ConstructorFunction<String, LocalTopicStatsImpl> localTopicStatsConstructorFunction = new ConstructorFunction<String, LocalTopicStatsImpl>() {
        public LocalTopicStatsImpl createNew(String mapName) {
            return new LocalTopicStatsImpl();
        }
    };

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        for (int i = 0; i < orderingLocks.length; i++) {
            orderingLocks[i] = new ReentrantLock();
        }
    }

    public void reset() {
        statsMap.clear();
    }

    public void shutdown() {
        reset();
    }

    public Lock getOrderLock(String key) {
        final int hash = key.hashCode();
        return orderingLocks[hash != Integer.MIN_VALUE ? (Math.abs(hash) % orderingLocks.length) : 0];
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public TopicProxy createDistributedObject(Object objectId) {
        final String name = String.valueOf(objectId);
        TopicProxy proxy;
        TopicConfig topicConfig = nodeEngine.getConfig().getTopicConfig(name);
        if (topicConfig.isGlobalOrderingEnabled())
            proxy = new TotalOrderedTopicProxy(name, nodeEngine, this);
        else
            proxy = new TopicProxy(name, nodeEngine, this);
        return proxy;
    }

    public void destroyDistributedObject(Object objectId) {
        statsMap.remove(String.valueOf(objectId));

    }

    public void dispatchEvent(Object event, Object listener) {
        TopicEvent topicEvent = (TopicEvent) event;
        Message message = new Message(topicEvent.name, nodeEngine.toObject(topicEvent.data), topicEvent.publishTime, topicEvent.publishingMember);
        incrementReceivedMessages(topicEvent.name);
        ((MessageListener) listener).onMessage(message);
    }

    public LocalTopicStatsImpl getLocalTopicStats(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(statsMap, name, statsMap, localTopicStatsConstructorFunction);
    }

    public void incrementPublishes(String topicName) {
        getLocalTopicStats(topicName).incrementPublishes();
    }

    public void incrementReceivedMessages(String topicName) {
        getLocalTopicStats(topicName).incrementReceives();
    }

    public void publishEvent(String name, TopicEvent event){
        EventService eventService = nodeEngine.getEventService();
        eventService.publishEvent(TopicService.SERVICE_NAME, eventService.getRegistrations(TopicService.SERVICE_NAME, name), event);
    }

    public String addMessageListener(String name, MessageListener listener){
        EventService eventService = nodeEngine.getEventService();
        EventRegistration eventRegistration = eventService.registerListener(TopicService.SERVICE_NAME, name, listener);
        return eventRegistration.getId();
    }

    public boolean removeMessageListener(String name, String registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(TopicService.SERVICE_NAME, name, registrationId);
    }

}
