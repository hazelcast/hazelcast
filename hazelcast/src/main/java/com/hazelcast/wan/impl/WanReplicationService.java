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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.version.Version;
import com.hazelcast.wan.DistributedServiceWanEventCounters;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.List;
import java.util.UUID;

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
    DelegatingWanReplicationScheme getWanReplicationPublishers(String name);

    /**
     * Starts the shutdown process of the WAN replication service.
     */
    void shutdown();

    /**
     * Pauses WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on this hazelcast instance.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param wanPublisherId     ID of the WAN replication publisher
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void pause(String wanReplicationName, String wanPublisherId);

    /**
     * Stops WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on this hazelcast instance.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param wanPublisherId     ID of the WAN replication publisher
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void stop(String wanReplicationName, String wanPublisherId);

    /**
     * Resumes WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on this hazelcast instance.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param wanPublisherId     ID of the WAN replication publisher
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void resume(String wanReplicationName, String wanPublisherId);

    /**
     * Initiate wan sync for a specific map.
     * NOTE: not supported on OS, only on EE
     *
     * @param wanReplicationName the name of the wan replication config
     * @param wanPublisherId     ID of the WAN replication publisher
     * @param mapName            the map name
     * @return the UUID of the synchronization
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication
     *                                       config for {@code wanReplicationName}
     */
    UUID syncMap(String wanReplicationName, String wanPublisherId, String mapName);

    /**
     * Initiate wan sync for all maps.
     * NOTE: not supported on OS, only on EE
     *
     * @param wanReplicationName the name of the wan replication config
     * @param wanPublisherId     ID of the WAN replication publisher
     * @return the UUID of the synchronization
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication config for
     *                                       {@code wanReplicationName}
     */
    UUID syncAllMaps(String wanReplicationName, String wanPublisherId);

    /**
     * Initiate WAN consistency check for a specific map.
     * NOTE: not supported on OS, only on EE
     *
     * @param wanReplicationName the name of the wan replication config
     * @param wanPublisherId     ID of the WAN replication publisher
     * @param mapName            the map name
     * @return the UUID of the consistency check request or {@code null}
     * if consistency check is ignored because of the configuration
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication config for {@code wanReplicationName}
     */
    UUID consistencyCheck(String wanReplicationName, String wanPublisherId, String mapName);

    /**
     * Removes all WAN events awaiting replication for the given wanReplicationName
     * for the given target.
     * If the publisher does not store WAN events, this method is a no-op.
     * Invoked when clearing the WAN replication data, e.g. because of a REST call.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param wanPublisherId     ID of the WAN replication publisher
     * @throws UnsupportedOperationException if invoked on OS
     */
    void removeWanEvents(String wanReplicationName, String wanPublisherId);

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
     * @param wanPublisherId     WAN replication publisher ID
     * @param serviceName        the name of the service for the WAN events
     * @return the WAN event counter
     */
    DistributedServiceWanEventCounters getSentEventCounters(String wanReplicationName,
                                                            String wanPublisherId,
                                                            String serviceName);

    /**
     * Removes all WAN event counters for the given {@code serviceName} and
     * {@code dataStructureName}.
     *
     * @param serviceName       the name of the service for the WAN events
     * @param dataStructureName the distributed object name
     */
    void removeWanEventCounters(String serviceName, String dataStructureName);

    /**
     * Returns an immutable collection of all WAN protocol versions supported by
     * this instance.
     */
    List<Version> getSupportedWanProtocolVersions();
}
