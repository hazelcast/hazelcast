/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.events.AddWanConfigIgnoredEvent;
import com.hazelcast.internal.management.events.WanConsistencyCheckIgnoredEvent;
import com.hazelcast.internal.management.events.WanSyncIgnoredEvent;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.AddWanConfigResult;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Open source implementation of the {@link com.hazelcast.wan.WanReplicationService}
 */
public class WanReplicationServiceImpl implements WanReplicationService {

    private final Node node;

    /** WAN event counters for all services and only received events */
    private final WanEventCounters receivedWanEventCounters = new WanEventCounters();

    /** WAN event counters for all services and only sent events */
    private final WanEventCounters sentWanEventCounters = new WanEventCounters();

    private final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications
            = initializeWanReplicationPublisherMapping();
    private final ConstructorFunction<String, WanReplicationPublisherDelegate> publisherDelegateConstructorFunction =
            new ConstructorFunction<String, WanReplicationPublisherDelegate>() {
                @Override
                public WanReplicationPublisherDelegate createNew(String name) {
                    final WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
                    if (wanReplicationConfig == null) {
                        return null;
                    }
                    final List<WanPublisherConfig> publisherConfigs = wanReplicationConfig.getWanPublisherConfigs();
                    return new WanReplicationPublisherDelegate(name, createPublishers(wanReplicationConfig, publisherConfigs));
                }
            };

    public WanReplicationServiceImpl(Node node) {
        this.node = node;
    }

    @Override
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        return getOrPutSynchronized(wanReplications, name, this, publisherDelegateConstructorFunction);
    }

    private WanReplicationEndpoint[] createPublishers(WanReplicationConfig wanReplicationConfig,
                                                      List<WanPublisherConfig> publisherConfigs) {
        WanReplicationEndpoint[] targetEndpoints = new WanReplicationEndpoint[publisherConfigs.size()];
        int count = 0;
        for (WanPublisherConfig publisherConfig : publisherConfigs) {
            final WanReplicationEndpoint target = getOrCreate((WanReplicationEndpoint) publisherConfig.getImplementation(),
                    node.getConfigClassLoader(),
                    publisherConfig.getClassName());
            if (target == null) {
                throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                        + "attribute need to be set in WanPublisherConfig");
            }
            target.init(node, wanReplicationConfig, publisherConfig);
            targetEndpoints[count++] = target;
        }
        return targetEndpoints;
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
                final WanReplicationEndpoint[] endpoints = wanReplication.getEndpoints();
                if (endpoints != null) {
                    for (WanReplicationEndpoint endpoint : endpoints) {
                        if (endpoint != null) {
                            endpoint.shutdown();
                        }
                    }
                }
            }
            wanReplications.clear();
        }
    }

    @Override
    public void pause(String wanReplicationName, String targetGroupName) {
        throw new UnsupportedOperationException("Pausing WAN replication is not supported.");
    }

    @Override
    public void stop(String wanReplicationName, String targetGroupName) {
        throw new UnsupportedOperationException("Stopping WAN replication is not supported");
    }

    @Override
    public void resume(String wanReplicationName, String targetGroupName) {
        throw new UnsupportedOperationException("Resuming WAN replication is not supported");
    }

    @Override
    public void checkWanReplicationQueues(String name) {
        //NOP
    }

    @Override
    public void syncMap(String wanReplicationName, String targetGroupName, String mapName) {
        node.getManagementCenterService().log(
                WanSyncIgnoredEvent.enterpriseOnly(wanReplicationName, targetGroupName, mapName));

        throw new UnsupportedOperationException("WAN sync for map is not supported.");
    }

    @Override
    public void syncAllMaps(String wanReplicationName, String targetGroupName) {
        node.getManagementCenterService().log(
                WanSyncIgnoredEvent.enterpriseOnly(wanReplicationName, targetGroupName, null));

        throw new UnsupportedOperationException("WAN sync is not supported.");
    }

    @Override
    public void consistencyCheck(String wanReplicationName, String targetGroupName, String mapName) {
        node.getManagementCenterService().log(
                new WanConsistencyCheckIgnoredEvent(wanReplicationName, targetGroupName, mapName,
                        "Consistency check is supported for enterprise clusters only."));

        throw new UnsupportedOperationException("Consistency check is not supported.");
    }

    @Override
    public void clearQueues(String wanReplicationName, String targetGroupName) {
        throw new UnsupportedOperationException("Clearing WAN replication queues is not supported.");
    }

    @Override
    public AddWanConfigResult addWanReplicationConfig(WanReplicationConfig wanConfig) {
        node.getManagementCenterService().log(AddWanConfigIgnoredEvent.enterpriseOnly(wanConfig.getName()));

        throw new UnsupportedOperationException("Adding new WAN config is not supported.");
    }

    @Override
    public void addWanReplicationConfigLocally(WanReplicationConfig wanConfig) {
        throw new UnsupportedOperationException("Adding new WAN config is not supported.");
    }

    @Override
    public Map<String, LocalWanStats> getStats() {
        return null;
    }

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWanReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }

    @Override
    public WanSyncState getWanSyncState() {
        return null;
    }

    @Override
    public DistributedServiceWanEventCounters getReceivedEventCounters(String serviceName) {
        return receivedWanEventCounters.getWanEventCounter("", "", serviceName);
    }

    @Override
    public DistributedServiceWanEventCounters getSentEventCounters(String wanReplicationName,
                                                                   String targetGroupName,
                                                                   String serviceName) {
        return sentWanEventCounters.getWanEventCounter(wanReplicationName, targetGroupName, serviceName);
    }

    @Override
    public void removeWanEventCounters(String serviceName, String objectName) {
        receivedWanEventCounters.removeCounter(serviceName, objectName);
        sentWanEventCounters.removeCounter(serviceName, objectName);
    }
}
