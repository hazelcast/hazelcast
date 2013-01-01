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

import com.hazelcast.cluster.FinalizeJoinOperation;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ServiceProxy;
import com.hazelcast.topic.TopicEvent;
import com.hazelcast.topic.TopicService;
import sun.jvmstat.perfdata.monitor.CountedTimerTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.spec.DSAPublicKeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 2:06 PM
 */
public class TopicProxy<E> implements ITopic<E>, ServiceProxy {

    private final String name;
    protected final NodeEngine nodeEngine;
    private final EventService eventService;

    private final ConcurrentMap<String, String> registeredIds = new ConcurrentHashMap<String, String>();

    public TopicProxy(String name, NodeEngine nodeEngine) {

        this.name = name;
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();

    }

    public String getName() {
        return name;
    }

    public void publish(E message) {
        TopicEvent topicEvent = new TopicEvent(name, nodeEngine.toData(message));
        eventService.publishEvent(TopicService.NAME, eventService.getRegistrations(TopicService.NAME, name), topicEvent);
    }

    public void addMessageListener(MessageListener<E> listener) {
        EventRegistration eventRegistration = eventService.registerListener(TopicService.NAME, name, listener);

        if (registeredIds.putIfAbsent(name, eventRegistration.getId()) != null)
            System.out.println("Already registered");
    }

    public void removeMessageListener(MessageListener<E> listener) {
        if (registeredIds.get(name) == null)
            System.out.println("Not registered");
        else {
            eventService.deregisterListener(TopicService.NAME, name, registeredIds.get(name));
            registeredIds.remove(name);
        }
    }

    public LocalTopicStats getLocalTopicStats() {
        return null;
    }

    public InstanceType getInstanceType() {
        return InstanceType.TOPIC;
    }

    public void destroy() {
        registeredIds.remove(name);
    }

    public Object getId() {
        return getName();
    }

}


