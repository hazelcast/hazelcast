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

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.spi.*;
import com.hazelcast.topic.client.TopicListenHandler;
import com.hazelcast.topic.client.TopicPublishHandler;
import com.hazelcast.topic.proxy.TopicProxy;
import com.hazelcast.topic.proxy.TotalOrderedTopicProxy;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.HashMap;
import java.util.Map;
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
public class TopicService implements ManagedService, RemoteService, EventPublishingService, ClientProtocolService {

    public static final String SERVICE_NAME = "hz:impl:topicService";
    private final Lock[] orderingLocks = new Lock[1000];
    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, TopicContainer> topicContainers = new ConcurrentHashMap<String, TopicContainer>();

    private final ConcurrencyUtil.ConstructorFunction<String, TopicContainer> topicConstructor = new ConcurrencyUtil.ConstructorFunction<String, TopicContainer>() {
        public TopicContainer createNew(String mapName) {
            return new TopicContainer();
        }
    };

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        for (int i = 0; i < orderingLocks.length; i++) {
            orderingLocks[i] = new ReentrantLock();
        }
    }

    public void reset() {
        topicContainers.clear();
    }

    public void shutdown() {
        reset();
    }

    public Lock getOrderLock(String key) {
        return orderingLocks[Math.abs(key.hashCode()) % orderingLocks.length];
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

    public TopicProxy createDistributedObjectForClient(Object objectId) {
        return createDistributedObject(objectId);
    }

    public void destroyDistributedObject(Object objectId) {
        topicContainers.remove(String.valueOf(objectId));

    }

    public void dispatchEvent(Object event, Object listener) {
        TopicEvent topicEvent = (TopicEvent) event;
        Message message = new Message(topicEvent.name, nodeEngine.toObject(topicEvent.data));
        incrementReceivedMessages(topicEvent.name);
        ((MessageListener) listener).onMessage(message);
    }

    public TopicContainer getAtomicLongContainer(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(topicContainers, name, topicContainers, topicConstructor);
    }

    public void incrementPublishes(String topicName) {
        topicContainers.get(topicName).incrementPublishes();
    }

    public void incrementReceivedMessages(String topicName) {
        topicContainers.get(topicName).incrementReceivedMessages();
    }

    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        Map<Command, ClientCommandHandler> map = new HashMap<Command, ClientCommandHandler>();
        map.put(Command.TPUBLISH, new TopicPublishHandler(this));
        map.put(Command.TLISTEN, new TopicListenHandler(this));
        return map;
    }

}
