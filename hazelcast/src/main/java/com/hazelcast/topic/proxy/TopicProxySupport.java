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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.topic.TopicEvent;
import com.hazelcast.topic.TopicService;

/**
 * User: sancar
 * Date: 2/26/13
 * Time: 11:44 AM
 */
public class TopicProxySupport extends AbstractDistributedObject<TopicService> {

    private final String name;
    private final EventService eventService;

    TopicProxySupport(String name, NodeEngine nodeEngine, TopicService service) {
        super(nodeEngine, service);
        this.name = name;
        eventService = nodeEngine.getEventService();

    }

    public LocalTopicStats getLocalTopicStatsInternal() {
        return getService().getLocalTopicStats(name);
    }

    public void publishInternal(Data message) {
        TopicEvent topicEvent = new TopicEvent(name, message, getNodeEngine().getLocalMember());
        getService().getLocalTopicStats(name).incrementPublishes();
        eventService.publishEvent(TopicService.SERVICE_NAME, eventService.getRegistrations(TopicService.SERVICE_NAME, name), topicEvent);
    }

    public String addMessageListenerInternal(MessageListener listener) {
        EventRegistration eventRegistration = eventService.registerListener(TopicService.SERVICE_NAME, name, listener);
        return eventRegistration.getId();
    }

    public boolean removeMessageListenerInternal(final String registrationId) {
        return eventService.deregisterListener(TopicService.SERVICE_NAME, name, registrationId);
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
