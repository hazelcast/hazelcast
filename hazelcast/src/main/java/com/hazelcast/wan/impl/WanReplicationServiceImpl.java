/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.events.WanAddConfigurationIgnoredEvent;
import com.hazelcast.internal.management.events.WanConsistencyCheckIgnoredEvent;
import com.hazelcast.internal.management.events.WanSyncIgnoredEvent;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanMigrationAwarePublisher;
import com.hazelcast.wan.WanPublisher;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Open source implementation of the {@link WanReplicationService}
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class WanReplicationServiceImpl implements WanReplicationService,
        ChunkedMigrationAwareService, ManagedService {

    private final Node node;

    /**
     * WAN event counters for all services and only received events
     */
    private final WanEventCounterRegistry receivedWanEventCounters = new WanEventCounterRegistry();

    /**
     * WAN event counters for all services and only sent events
     */
    private final WanEventCounterRegistry sentWanEventCounters = new WanEventCounterRegistry();

    private final ConcurrentMap<String, DelegatingWanScheme> wanReplications = createConcurrentHashMap(1);

    private final ConstructorFunction<String, DelegatingWanScheme> publisherDelegateConstructor;

    public WanReplicationServiceImpl(Node node) {
        this.node = node;
        this.publisherDelegateConstructor = name -> {
            final WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
            if (wanReplicationConfig == null) {
                return null;
            }
            List<WanBatchPublisherConfig> batchPublisherConfigs
                    = wanReplicationConfig.getBatchPublisherConfigs();
            if (!batchPublisherConfigs.isEmpty()) {
                throw new InvalidConfigurationException("Built-in batching WAN replication implementation "
                        + "is only available in Hazelcast enterprise edition.");
            }
            return new DelegatingWanScheme(name, createPublishers(wanReplicationConfig));
        };
    }

    @Override
    public DelegatingWanScheme getWanReplicationPublishers(String wanReplicationScheme) {
        if (!wanReplications.containsKey(wanReplicationScheme)
                && node.getConfig().getWanReplicationConfig(wanReplicationScheme) == null) {
            return null;
        }
        return getOrPutSynchronized(wanReplications, wanReplicationScheme, this, publisherDelegateConstructor);
    }

    private ConcurrentMap<String, WanPublisher> createPublishers(WanReplicationConfig wanReplicationConfig) {
        List<WanCustomPublisherConfig> customPublisherConfigs = wanReplicationConfig.getCustomPublisherConfigs();
        int publisherCount = customPublisherConfigs.size();

        if (publisherCount == 0) {
            return createConcurrentHashMap(1);
        }

        ConcurrentMap<String, WanPublisher> publishers = createConcurrentHashMap(publisherCount);
        Map<String, AbstractWanPublisherConfig> publisherConfigs = createHashMap(publisherCount);

        customPublisherConfigs.forEach(
                publisherConfig -> {
                    String publisherId = getWanPublisherId(publisherConfig);
                    if (publishers.containsKey(publisherId)) {
                        throw new InvalidConfigurationException(
                                "Detected duplicate publisher ID '" + publisherId + "' for a single WAN replication config");
                    }

                    WanPublisher publisher = createPublisher(publisherConfig);
                    publishers.put(publisherId, publisher);
                    publisherConfigs.put(publisherId, publisherConfig);
                });

        for (Entry<String, WanPublisher> publisherEntry : publishers.entrySet()) {
            String publisherId = publisherEntry.getKey();
            WanPublisher publisher = publisherEntry.getValue();
            ManagedContext managedContext = node.getSerializationService().getManagedContext();
            publisher = (WanPublisher) managedContext.initialize(publisher);
            publisher.init(wanReplicationConfig, publisherConfigs.get(publisherId));
        }

        return publishers;
    }

    /**
     * Instantiates a {@link WanPublisher} from the provided publisher
     * configuration.
     *
     * @param publisherConfig the WAN publisher configuration
     * @return the WAN replication publisher
     * @throws InvalidConfigurationException if the method was unable to create the publisher because there was no
     *                                       implementation or class name defined on the config
     */
    private WanPublisher createPublisher(AbstractWanPublisherConfig publisherConfig) {
        WanPublisher publisher = getOrCreate(
                publisherConfig.getImplementation(),
                node.getConfigClassLoader(),
                publisherConfig.getClassName());
        if (publisher == null) {
            throw new InvalidConfigurationException("Either 'implementation' or 'className' "
                    + "attribute need to be set in the WAN publisher configuration for publisher " + publisherConfig);
        }
        return publisher;
    }

    /**
     * Returns the publisher ID for the given WAN publisher configuration which
     * is then used for identifying the WAN publisher in a WAN replication
     * scheme.
     * If the publisher ID is empty, returns the publisher group name.
     *
     * @param publisherConfig the WAN replication publisher configuration
     * @return the WAN publisher ID
     */
    public static @Nonnull
    String getWanPublisherId(AbstractWanPublisherConfig publisherConfig) {
        String publisherId = null;
        if (!isNullOrEmptyAfterTrim(publisherConfig.getPublisherId())) {
            publisherId = publisherConfig.getPublisherId();
        } else if (publisherConfig instanceof WanBatchPublisherConfig) {
            publisherId = ((WanBatchPublisherConfig) publisherConfig).getClusterName();
        }
        if (publisherId == null) {
            throw new InvalidConfigurationException("Publisher ID or group name is not specified for " + publisherConfig);
        }
        return publisherId;
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            for (DelegatingWanScheme delegate : wanReplications.values()) {
                for (WanPublisher publisher : delegate.getPublishers()) {
                    if (publisher != null) {
                        publisher.shutdown();
                    }
                }
            }
            wanReplications.clear();
        }
    }

    @Override
    public void pause(String wanReplicationName, String wanPublisherId) {
        throw new UnsupportedOperationException("Pausing WAN replication is not supported.");
    }

    @Override
    public void stop(String wanReplicationName, String wanPublisherId) {
        throw new UnsupportedOperationException("Stopping WAN replication is not supported");
    }

    @Override
    public void resume(String wanReplicationName, String wanPublisherId) {
        throw new UnsupportedOperationException("Resuming WAN replication is not supported");
    }

    @Override
    public UUID syncMap(String wanReplicationName, String wanPublisherId, String mapName) {
        node.getManagementCenterService().log(
                WanSyncIgnoredEvent.enterpriseOnly(wanReplicationName, wanPublisherId, mapName));
        throw new UnsupportedOperationException("WAN sync for map is not supported.");
    }

    @Override
    public UUID syncAllMaps(String wanReplicationName, String wanPublisherId) {
        node.getManagementCenterService().log(
                WanSyncIgnoredEvent.enterpriseOnly(wanReplicationName, wanPublisherId, null));
        throw new UnsupportedOperationException("WAN sync is not supported.");
    }

    @Override
    public UUID consistencyCheck(String wanReplicationName, String wanPublisherId, String mapName) {
        node.getManagementCenterService().log(
                WanConsistencyCheckIgnoredEvent.enterpriseOnly(wanReplicationName, wanPublisherId, mapName));

        throw new UnsupportedOperationException("Consistency check is not supported.");
    }

    @Override
    public void removeWanEvents(String wanReplicationName, String wanPublisherId) {
        throw new UnsupportedOperationException("Clearing WAN replication queues is not supported.");
    }

    @Override
    public AddWanConfigResult addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        node.getManagementCenterService().log(WanAddConfigurationIgnoredEvent.enterpriseOnly(wanReplicationConfig.getName()));

        throw new UnsupportedOperationException("Adding new WAN config is not supported.");
    }

    @Override
    public void addWanReplicationConfigLocally(WanReplicationConfig wanReplicationConfig) {
        throw new UnsupportedOperationException("Adding new WAN config is not supported.");
    }

    @Override
    public Map<String, LocalWanStats> getStats() {
        return null;
    }

    @Override
    public WanSyncState getWanSyncState() {
        return null;
    }

    @Override
    public WanEventCounters getReceivedEventCounters(String serviceName) {
        return receivedWanEventCounters.getWanEventCounter("", "", serviceName);
    }

    @Override
    public WanEventCounters getSentEventCounters(String wanReplicationName,
                                                 String wanPublisherId,
                                                 String serviceName) {
        return sentWanEventCounters.getWanEventCounter(wanReplicationName, wanPublisherId, serviceName);
    }

    @Override
    public void removeWanEventCounters(String serviceName, String objectName) {
        receivedWanEventCounters.removeCounter(serviceName, objectName);
        sentWanEventCounters.removeCounter(serviceName, objectName);
    }

    @Override
    public List<Version> getSupportedWanProtocolVersions() {
        // EE WAN replication not supported in OS
        return Collections.emptyList();
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationStart(event));
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationCommit(event));
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationRollback(event));
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // NOP
    }

    @Override
    public void reset() {
        final Collection<DelegatingWanScheme> publishers = wanReplications.values();
        for (DelegatingWanScheme publisherDelegate : publishers) {
            for (WanPublisher publisher : publisherDelegate.getPublishers()) {
                publisher.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        if (wanReplications.isEmpty()) {
            return Collections.emptyList();
        }

        Set<ServiceNamespace> namespaces = new HashSet<>();
        for (DelegatingWanScheme publisher : wanReplications.values()) {
            publisher.collectAllServiceNamespaces(event, namespaces);
        }
        return namespaces;
    }

    /**
     * {@inheritDoc}
     * On OS side we emit WAN events only for MapService.
     * On EE side we emit events for MapService and CacheService.
     */
    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        final String serviceName = namespace.getServiceName();
        return namespace instanceof ObjectNamespace && (MapService.SERVICE_NAME.equals(serviceName));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        if (wanReplications.isEmpty() || namespaces.isEmpty()) {
            return null;
        }
        Map<String, Map<String, Object>> eventContainers = createHashMap(wanReplications.size());

        for (Entry<String, DelegatingWanScheme> wanReplicationEntry : wanReplications.entrySet()) {
            String replicationScheme = wanReplicationEntry.getKey();
            DelegatingWanScheme delegate = wanReplicationEntry.getValue();
            Map<String, Object> publisherEventContainers = delegate.prepareEventContainerReplicationData(event, namespaces);
            if (!publisherEventContainers.isEmpty()) {
                eventContainers.put(replicationScheme, publisherEventContainers);
            }
        }

        if (eventContainers.isEmpty()) {
            return null;
        } else {
            return new WanEventContainerReplicationOperation(
                    Collections.emptyList(), eventContainers, event.getPartitionId(), event.getReplicaIndex());
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public WanPublisher getPublisherOrFail(String wanReplicationName,
                                           String wanPublisherId) {
        WanPublisher publisher = getPublisherOrNull(wanReplicationName, wanPublisherId);
        if (publisher == null) {
            throw new InvalidConfigurationException("WAN Replication Config doesn't exist with WAN configuration name "
                    + wanReplicationName + " and publisher ID " + wanPublisherId);
        }
        return publisher;
    }

    @Override
    public boolean appendWanReplicationConfig(WanReplicationConfig newConfig) {
        // not implemented in OS
        return false;
    }

    private WanPublisher getPublisherOrNull(String wanReplicationName,
                                            String wanPublisherId) {
        DelegatingWanScheme publisherDelegate = getWanReplicationPublishers(wanReplicationName);
        return publisherDelegate != null
                ? publisherDelegate.getPublisher(wanPublisherId)
                : null;
    }

    private void notifyMigrationAwarePublishers(Consumer<WanMigrationAwarePublisher> publisherConsumer) {
        for (DelegatingWanScheme wanReplication : wanReplications.values()) {
            for (WanPublisher publisher : wanReplication.getPublishers()) {
                if (publisher instanceof WanMigrationAwarePublisher) {
                    publisherConsumer.accept((WanMigrationAwarePublisher) publisher);
                }
            }
        }
    }
}
