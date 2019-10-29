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

package com.hazelcast.wan.impl;

import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * WAN replication scheme implementation delegating to multiple WAN
 * replication publisher implementations. This implementation is a container
 * for multiple WAN publishers.
 * When publishing an event on this delegate, all publishers are notified.
 */
public final class DelegatingWanReplicationScheme {
    /** Non-null WAN replication name */
    final String name;
    /** Non-null WAN publishers, grouped by publisher ID */
    final ConcurrentMap<String, WanReplicationPublisher> publishers;

    public DelegatingWanReplicationScheme(@Nonnull String name,
                                          @Nonnull ConcurrentMap<String, WanReplicationPublisher> publishers) {
        checkNotNull(name, "WAN publisher name should not be null");
        checkNotNull(publishers, "WAN publisher map should not be null");
        this.name = name;
        this.publishers = publishers;
    }

    /** Returns all {@link WanReplicationPublisher}s for this delegate */
    public @Nonnull
    Collection<WanReplicationPublisher> getPublishers() {
        return publishers.values();
    }

    /**
     * Returns the {@link WanReplicationPublisher} with the {@code publisherId}
     * or {@code null} if it doesn't exist.
     */
    public WanReplicationPublisher getPublisher(String publisherId) {
        return publishers.get(publisherId);
    }

    public void addPublisher(@Nonnull String publisherId,
                             @Nonnull WanReplicationPublisher publisher) {
        if (publishers.putIfAbsent(publisherId, publisher) != null) {
            throw new IllegalStateException("Publisher with publisher ID " + publisherId
                    + " on WAN replication scheme " + name + " is already present and cannot be overriden");
        }
    }

    public String getName() {
        return name;
    }

    /**
     * Publishes a replication event to all publishers to which this publisher
     * delegates.
     */
    public void publishReplicationEvent(WanReplicationEvent event) {
        for (WanReplicationPublisher publisher : publishers.values()) {
            publisher.publishReplicationEvent(event);
        }
    }

    /**
     * Publishes a backup replication event to all publishers to which this
     * publisher delegates.
     */
    public void publishReplicationEventBackup(WanReplicationEvent event) {
        for (WanReplicationPublisher publisher : publishers.values()) {
            publisher.publishReplicationEventBackup(event);
        }
    }

    /**
     * Publishes a replication event to all publishers to which this publisher
     * delegates.
     */
    public void republishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        for (WanReplicationPublisher publisher : publishers.values()) {
            publisher.republishReplicationEvent(wanReplicationEvent);
        }
    }

    public Map<String, LocalWanPublisherStats> getStats() {
        final Map<String, LocalWanPublisherStats> statsMap = createHashMap(publishers.size());
        for (Entry<String, WanReplicationPublisher> publisherEntry : publishers.entrySet()) {
            String publisherId = publisherEntry.getKey();
            WanReplicationPublisher publisher = publisherEntry.getValue();
            LocalWanPublisherStats stats = publisher.getStats();
            if (stats != null) {
                statsMap.put(publisherId, stats);
            }
        }
        return statsMap;
    }

    public void doPrepublicationChecks() {
        for (WanReplicationPublisher publisher : publishers.values()) {
            publisher.doPrepublicationChecks();
        }
    }

    /**
     * Collect all replication data matching the replication event and collection
     * of namespaces being replicated.
     * Returns containers for WAN replication events grouped by WAN publisher ID.
     *
     * @param event      the replication event
     * @param namespaces the object namespaces which are being replicated
     * @return a map from WAN publisher ID to container object for WAN replication events
     */
    public Map<String, Object> prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                                    Collection<ServiceNamespace> namespaces) {
        Map<String, Object> eventContainers = createHashMap(publishers.size());
        for (Entry<String, WanReplicationPublisher> publisherEntry : publishers.entrySet()) {
            Object eventContainer = publisherEntry.getValue()
                                                  .prepareEventContainerReplicationData(event, namespaces);
            if (eventContainer != null) {
                String publisherId = publisherEntry.getKey();
                eventContainers.put(publisherId, eventContainer);
            }
        }
        return eventContainers;
    }

    /**
     * Collect the namespaces of all queues that should be replicated by the
     * replication event.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    public void collectAllServiceNamespaces(PartitionReplicationEvent event,
                                            Set<ServiceNamespace> namespaces) {
        for (WanReplicationPublisher publisher : publishers.values()) {
            publisher.collectAllServiceNamespaces(event, namespaces);
        }
    }

    /**
     * Releases all resources for the map with the given {@code mapName}.
     *
     * @param mapName the map mapName
     */
    public void destroyMapData(String mapName) {
        for (WanReplicationPublisher publisher : publishers.values()) {
            if (publisher instanceof InternalWanReplicationPublisher) {
                ((InternalWanReplicationPublisher) publisher).destroyMapData(mapName);
            }
        }
    }
}
