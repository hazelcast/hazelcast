/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.MapUtil;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

public class TopicService implements ManagedService, RemoteService, EventPublishingService,
        StatisticsAwareService<LocalTopicStats> {

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
    private final AtomicInteger counter = new AtomicInteger(0);
    private Address localAddress;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.localAddress = nodeEngine.getThisAddress();
        for (int i = 0; i < orderingLocks.length; i++) {
            orderingLocks[i] = new ReentrantLock();
        }
        eventService = nodeEngine.getEventService();
    }

    // only for testing
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
        return HashUtil.hashToIndex(hash, orderingLocks.length);
    }

    @Override
    public ITopic createDistributedObject(String name, boolean local) {
        TopicConfig topicConfig = nodeEngine.getConfig().findTopicConfig(name);

        if (topicConfig.isGlobalOrderingEnabled()) {
            return new TotalOrderedTopicProxy(name, nodeEngine, this);
        } else {
            return new TopicProxy(name, nodeEngine, this);
        }
    }

    @Override
    public void destroyDistributedObject(String objectId, boolean local) {
        statsMap.remove(objectId);
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, objectId);
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        TopicEvent topicEvent = (TopicEvent) event;
        ClusterService clusterService = nodeEngine.getClusterService();
        MemberImpl member = clusterService.getMember(topicEvent.publisherAddress);
        if (member == null) {
            member = new MemberImpl.Builder(topicEvent.publisherAddress)
                    .version(nodeEngine.getVersion())
                    .build();
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

    /**
     * Increments the number of published messages on the ITopic
     * with the name {@code topicName}.
     *
     * @param topicName the name of the {@link ITopic}
     */
    public void incrementPublishes(String topicName) {
        getLocalTopicStats(topicName).incrementPublishes();
    }

    /**
     * Increments the number of received messages on the ITopic
     * with the name {@code topicName}.
     *
     * @param topicName the name of the {@link ITopic}
     */
    public void incrementReceivedMessages(String topicName) {
        getLocalTopicStats(topicName).incrementReceives();
    }

    public void publishMessage(String topicName, Object payload, boolean multithreaded) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, topicName);
        if (!registrations.isEmpty()) {
            Data payloadData = nodeEngine.toData(payload);
            TopicEvent topicEvent = new TopicEvent(topicName, payloadData, localAddress);
            int partitionId = multithreaded ? counter.incrementAndGet() : topicName.hashCode();
            eventService.publishEvent(SERVICE_NAME, registrations, topicEvent, partitionId);
        }
    }

    public @Nonnull
    UUID addMessageListener(@Nonnull String name,
                              @Nonnull MessageListener listener,
                              boolean localOnly) {
        EventRegistration eventRegistration;
        if (localOnly) {
            eventRegistration = eventService.registerLocalListener(TopicService.SERVICE_NAME, name, listener);
        } else {
            eventRegistration = eventService.registerListener(TopicService.SERVICE_NAME, name, listener);

        }
        return eventRegistration.getId();
    }

    public boolean removeMessageListener(@Nonnull String name, @Nonnull UUID registrationId) {
        return eventService.deregisterListener(TopicService.SERVICE_NAME, name, registrationId);
    }

    @Override
    public Map<String, LocalTopicStats> getStats() {
        Map<String, LocalTopicStats> topicStats = MapUtil.createHashMap(statsMap.size());
        for (Map.Entry<String, LocalTopicStatsImpl> queueStat : statsMap.entrySet()) {
            topicStats.put(queueStat.getKey(), queueStat.getValue());
        }
        return topicStats;
    }
}
