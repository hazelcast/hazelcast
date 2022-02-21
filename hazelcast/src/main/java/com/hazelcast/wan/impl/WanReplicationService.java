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
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanPublisher;

import java.util.List;
import java.util.UUID;

/**
 * This is the WAN replications service API core interface. The
 * WanReplicationService needs to be capable of creating the actual
 * {@link WanPublisher} instances to replicate
 * values to other clusters over the wide area network, so it has to deal
 * with long delays, slow uploads and higher latencies.
 */
public interface WanReplicationService extends CoreService,
        StatisticsAwareService<LocalWanStats> {

    /**
     * Service name.
     */
    String SERVICE_NAME = "hz:core:wanReplicationService";

    /**
     * Returns a WAN replication configured under a WAN replication config with
     * the name {@code wanReplicationName} and with a WAN publisher ID of
     * {@code wanPublisherId} or throws a {@link InvalidConfigurationException}
     * if there is no configuration for the given parameters.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param wanPublisherId     WAN replication publisher ID
     * @return the WAN publisher
     * @throws InvalidConfigurationException if there is no replication config
     *                                       with the name {@code wanReplicationName}
     *                                       and publisher ID {@code wanPublisherId}
     * @see WanReplicationConfig#getName
     * @see WanBatchPublisherConfig#getClusterName()
     * @see AbstractWanPublisherConfig#getPublisherId()
     */
    WanPublisher getPublisherOrFail(String wanReplicationName,
                                    String wanPublisherId);

    /**
     * Appends the provided {@link WanReplicationConfig} to the configuration of
     * this member. If there is no WAN replication config with the same name/scheme,
     * the provided config will be used. If there is an existing WAN replication
     * config with the same name, any publishers with publisher IDs that are
     * present in the provided {@code newConfig} but not present in the existing
     * config will be added (appended).
     * If the existing config contains all the publishers from the provided
     * config, no change is done to the existing config.
     * This method is thread-safe and may be called concurrently.
     *
     * @param newConfig the WAN configuration to add
     * @return <code>false</code> if there is no change to the existing config
     * @see AbstractWanPublisherConfig#getPublisherId()
     */
    boolean appendWanReplicationConfig(WanReplicationConfig newConfig);

    /**
     * Creates a new {@link WanPublisher} by the given name. If
     * the name already exists, returns the previous instance.
     *
     * @param name name of the WAN replication configuration
     * @return instance of the corresponding replication publisher
     */
    DelegatingWanScheme getWanReplicationPublishers(String name);

    /**
     * Starts the shutdown process of the WAN replication service.
     */
    void shutdown();

    /**
     * Pauses WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on this hazelcast instance.
     * Silently skips publishers not supporting pausing.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param wanPublisherId     ID of the WAN replication publisher
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void pause(String wanReplicationName, String wanPublisherId);

    /**
     * Stops WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on this hazelcast instance.
     * Silently skips publishers not supporting stopping.
     *
     * @param wanReplicationName name of WAN replication configuration
     * @param wanPublisherId     ID of the WAN replication publisher
     * @throws UnsupportedOperationException if called on an OS instance
     */
    void stop(String wanReplicationName, String wanPublisherId);

    /**
     * Resumes WAN replication for the given {@code wanReplicationName} and
     * {@code wanPublisherId} on this hazelcast instance.
     * Silently skips publishers not supporting resuming.
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
     * Adds a new {@link WanReplicationConfig} to this member or appends to an
     * existing config and initializes any {@link WanPublisher}s that were added
     * as part of this config.
     * This method can also accept WAN configs with an existing WAN replication
     * name. Such configs will be merged into the existing WAN replication
     * config by adding publishers with publisher IDs which are not already part
     * of the existing configuration.
     * Publishers with IDs which already exist in the configuration are ignored.
     * In this sense, calling this method with the exact same config is thread-safe
     * and idempotent.
     *
     * @throws UnsupportedOperationException if invoked on OS
     */
    void addWanReplicationConfigLocally(WanReplicationConfig wanReplicationConfig);

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
     * @param wanReplicationConfig the WAN replication config to add
     * @return a best-effort guess at the result of adding WAN replication config
     * @throws UnsupportedOperationException if invoked on OS
     * @see #addWanReplicationConfigLocally(WanReplicationConfig)
     */
    AddWanConfigResult addWanReplicationConfig(WanReplicationConfig wanReplicationConfig);

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
    WanEventCounters getReceivedEventCounters(String serviceName);

    /**
     * Returns a counter of sent and processed WAN replication events.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param wanPublisherId     WAN replication publisher ID
     * @param serviceName        the name of the service for the WAN events
     * @return the WAN event counter
     */
    WanEventCounters getSentEventCounters(String wanReplicationName,
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
