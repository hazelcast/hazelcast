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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.topic.TopicEvent;
import com.hazelcast.topic.TopicService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 2:06 PM
 */
public class TopicProxy<E> extends AbstractDistributedObject implements ITopic<E> {

    private final String name;
    private final EventService eventService;
    private final ILogger logger = Logger.getLogger(TopicProxy.class.getName());
    private final ConcurrentMap<MessageListener, String> registeredIds = new ConcurrentHashMap<MessageListener, String>();

    public TopicProxy(String name, NodeEngine nodeEngine) {
        super(nodeEngine);
        this.name = name;
        this.eventService = nodeEngine.getEventService();
    }

    public String getName() {
        return name;
    }

    public void publish(E message) {
        TopicEvent topicEvent = new TopicEvent(name, nodeEngine.toData(message));
        eventService.publishEvent(TopicService.SERVICE_NAME, eventService.getRegistrations(TopicService.SERVICE_NAME, name), topicEvent);
    }

    public void addMessageListener(MessageListener<E> listener) {
        EventRegistration eventRegistration = eventService.registerListener(TopicService.SERVICE_NAME, name, listener);
        String currentId = registeredIds.put(listener, eventRegistration.getId());
        if (currentId != null) {
            eventService.deregisterListener(TopicService.SERVICE_NAME, name, currentId);
        }
    }

    public void removeMessageListener(MessageListener<E> listener) {
        String id = registeredIds.remove(listener);
        if (id != null) {
            eventService.deregisterListener(TopicService.SERVICE_NAME, name, id);
        }
    }

    public LocalTopicStats getLocalTopicStats() {
        return null;
    }

    public Object getId() {
        return getName();
    }

    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }
}


