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

package com.hazelcast.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

/**
 * This is the WAN replications service API core interface. The
 * WanReplicationService needs to be capable of creating the actual
 * {@link com.hazelcast.wan.WanReplicationPublisher} instances to replicate
 * values to other clusters over the wide area network, so it has to deal
 * with long delays, slow uploads and higher latencies.
 */
public interface WanReplicationService extends CoreService, StatisticsAwareService<LocalWanStats> {

    /**
     * Service name.
     */
    String SERVICE_NAME = "hz:core:wanReplicationService";

    /**
     * Creates a new {@link com.hazelcast.wan.WanReplicationPublisher} by the given name. If
     * the name already exists, returns the previous instance.
     *
     * @param name name of the WAN replication configuration
     * @return instance of the corresponding replication publisher
     */
    WanReplicationPublisher getWanReplicationPublisher(String name);

    /**
     * Starts the shutdown process of the WAN replication service.
     */
    void shutdown();

    /**
     * Pauses WAN replication for the given {@code wanReplicationName} and
     * {@code targetGroupName} on this hazelcast instance.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param targetGroupName    WAN target cluster group name
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void pause(String wanReplicationName, String targetGroupName);

    /**
     * Stops WAN replication for the given {@code wanReplicationName} and
     * {@code targetGroupName} on this hazelcast instance.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param targetGroupName    WAN target cluster group name
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void stop(String wanReplicationName, String targetGroupName);

    /**
     * Resumes WAN replication for the given {@code wanReplicationName} and
     * {@code targetGroupName} on this hazelcast instance.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param targetGroupName    WAN target cluster group name
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void resume(String wanReplicationName, String targetGroupName);

    void checkWanReplicationQueues(String name);

    /**
     * Initiate wan sync for a specific map.
     * NOTE: not supported on OS, only on EE
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @param mapName            the map name
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication
     *                                       config for {@code wanReplicationName}
     * @throws SyncFailedException           if there is a anti-entropy request in
     *                                       progress
     */
    void syncMap(String wanReplicationName, String targetGroupName, String mapName);

    /**
     * Initiate wan sync for all maps.
     * NOTE: not supported on OS, only on EE
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication config for
     *                                       {@code wanReplicationName}
     * @throws SyncFailedException           if there is a anti-entropy request in progress
     */
    void syncAllMaps(String wanReplicationName, String targetGroupName);


    /**
     * Initiate WAN consistency check for a specific map.
     * NOTE: not supported on OS, only on EE
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @param mapName            the map name
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication config for {@code wanReplicationName}
     * @throws SyncFailedException           if there is a anti-entropy request in progress
     */
    void consistencyCheck(String wanReplicationName, String targetGroupName, String mapName);


    /**
     * Clears WAN replication queues of the given wanReplicationName for the given target.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the target cluster group name
     * @throws UnsupportedOperationException if invoked on OS
     */
    void clearQueues(String wanReplicationName, String targetGroupName);

    /**
     * Adds a new {@link WanReplicationConfig} to this member and creates the
     * {@link WanReplicationPublisher}s specified in the config.
     * This method can also accept WAN configs with an existing WAN replication
     * name. Such configs will be merged into the existing WAN replication
     * config by adding publishers with publisher IDs which are not already part
     * of the existing configuration.
     *
     * @throws UnsupportedOperationException if invoked on OS
     */
    void addWanReplicationConfigLocally(WanReplicationConfig wanConfig);

    /**
     * Adds a new {@link WanReplicationConfig} to the cluster.
     * This method can also accept WAN configs with an existing WAN replication
     * name. Such configs will be merged into the existing WAN replication
     * config by adding publishers with publisher IDs which are not already part
     * of the existing configuration.
     * The return value is a best-effort guess at the result of adding WAN
     * replication config based on the existing local WAN replication config.
     * An exact result is difficult to calculate since not all members might
     * have the same existing configuration and there might be a concurrent
     * request to add overlapping WAN replication config.
     *
     * @param wanConfig the WAN replication config to add
     * @return a best-effort guess at the result of adding WAN replication config
     * @throws UnsupportedOperationException if invoked on OS
     * @see #addWanReplicationConfigLocally(WanReplicationConfig)
     */
    AddWanConfigResult addWanReplicationConfig(WanReplicationConfig wanConfig);

    /**
     * Returns current status of WAN sync operation or {@code null} when there
     * is no status.
     */
    WanSyncState getWanSyncState();

    /**
     * Returns a counter of received and processed WAN replication events.
     *
     * @param serviceName the name of the service for the WAN events
     * @return the WAN event counter
     */
    DistributedServiceWanEventCounters getReceivedEventCounters(String serviceName);

    /**
     * Returns a counter of sent and processed WAN replication events.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the target cluster group name
     * @param serviceName        the name of the service for the WAN events
     * @return the WAN event counter
     */
    DistributedServiceWanEventCounters getSentEventCounters(String wanReplicationName,
                                                            String targetGroupName,
                                                            String serviceName);

    /**
     * Removes all WAN event counters for the given {@code serviceName} and
     * {@code dataStructureName}.
     *
     * @param serviceName       the name of the service for the WAN events
     * @param dataStructureName the distributed object name
     */
    void removeWanEventCounters(String serviceName, String dataStructureName);
}
