/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.topic.TopicStatsContainer;
import com.hazelcast.topic.TopicEvent;
import com.hazelcast.topic.TopicService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: sancar
 * Date: 2/26/13
 * Time: 11:44 AM
 */
public class TopicProxySupport extends AbstractDistributedObject<TopicService> {

    private final String name;
    private final EventService eventService;
    private final ConcurrentMap<MessageListener, String> registeredIds = new ConcurrentHashMap<MessageListener, String>();

    TopicProxySupport(String name, NodeEngine nodeEngine, TopicService service) {
        super(nodeEngine, service);
        service.getTopicStatsContainer(name);
        this.name = name;
        eventService = nodeEngine.getEventService();

    }

    public LocalTopicStats getLocalTopicStatsInternal() {
        LocalTopicStatsImpl localTopicStats = new LocalTopicStatsImpl();
        TopicStatsContainer topicStatsContainer = getService().getTopicStatsContainer(name);
        localTopicStats.setCreationTime(topicStatsContainer.getCreationTime());
        localTopicStats.setTotalReceivedMessages(topicStatsContainer.getTotalReceivedMessages());
        localTopicStats.setTotalPublishes(topicStatsContainer.getTotalPublishes());
        localTopicStats.setLastPublishTime(topicStatsContainer.getLastAccessTime());
        localTopicStats.setOperationStats(topicStatsContainer.getOperationsCounter().getPublishedStats());
        return localTopicStats;
    }

    public void publishInternal(Data message) {
        TopicEvent topicEvent = new TopicEvent(name, message, getNodeEngine().getLocalMember());
        eventService.publishEvent(TopicService.SERVICE_NAME, eventService.getRegistrations(TopicService.SERVICE_NAME, name), topicEvent);
    }

    public void addMessageListenerInternal(MessageListener listener) {
        EventRegistration eventRegistration = eventService.registerListener(TopicService.SERVICE_NAME, name, listener);
        String currentId = registeredIds.put(listener, eventRegistration.getId());
        if (currentId != null) {
            eventService.deregisterListener(TopicService.SERVICE_NAME, name, currentId);
        }
    }

    public void removeMessageListenerInternal(MessageListener listener) {
        String id = registeredIds.remove(listener);
        if (id != null) {
            eventService.deregisterListener(TopicService.SERVICE_NAME, name, id);
        }
    }

    @Override
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
