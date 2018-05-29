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
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.util.Clock;
import com.hazelcast.wan.impl.WanEventCounter;

/**
 * This is the WAN replications service API core interface. The WanReplicationService needs to
 * be capable of creating the actual {@link com.hazelcast.wan.WanReplicationPublisher} instances
 * to replicate values to other clusters over the wide area network, so it has to deal with long
 * delays, slow uploads and higher latencies.
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
     * Pauses wan replication to target group for the called node
     *
     * @param name            name of WAN replication configuration
     * @param targetGroupName name of wan target cluster config
     */
    void pause(String name, String targetGroupName);

    /**
     * Resumes wan replication to target group for the called node.
     *
     * @param name            name of WAN replication configuration
     * @param targetGroupName name of wan target cluster config
     */
    void resume(String name, String targetGroupName);

    void checkWanReplicationQueues(String name);

    /**
     * Initiate WAN sync for a specific map.
     * <p>
     * NOTE: This method will throw a {@link UnsupportedOperationException} if
     * invoked on OS.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @param mapName            the map name
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication config for {@code wanReplicationName}
     * @throws SyncFailedException           if there is a sync request in progress
     */
    void syncMap(String wanReplicationName, String targetGroupName, String mapName);

    /**
     * Initiate WAN sync for all maps.
     * <p>
     * NOTE: This method will throw a {@link UnsupportedOperationException} if
     * invoked on OS.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication
     *                                       config for {@code wanReplicationName}
     * @throws SyncFailedException           if there is a sync request in progress
     */
    void syncAllMaps(String wanReplicationName, String targetGroupName);

    /**
     * Initiate WAN sync for a specific map.
     * This method may sync only some entries which have been updated in a
     * certain time interval. The interval is defined by the {@code fromTimestamp}
     * and {@code toTimestamp} parameters. These parameters represent the
     * timestamp as defined by the {@link Clock#currentTimeMillis()} method.
     * <p>
     * NOTE: This method will throw a {@link UnsupportedOperationException} if
     * invoked on OS or if the cluster version is less than 3.9.
     * <p>
     * If the cluster contains members with versions less than 3.9.5, this
     * invocation will pass but the WAN sync will fail.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @param mapName            the map name
     * @param fromTimestamp      the earliest time since when the entry must have been changed
     * @param toTimestamp        the latest time until when the entry must have been changed
     * @throws UnsupportedOperationException if the operation is not supported
     *                                       (not EE) or when the cluster version is less than
     *                                       {@link com.hazelcast.internal.cluster.Versions#CURRENT_CLUSTER_VERSION}
     * @throws InvalidConfigurationException if there is no WAN replication
     *                                       config for {@code wanReplicationName}
     * @throws SyncFailedException           if there is a sync request in progress
     * @see Clock#currentTimeMillis()
     * @see ClusterService#getClusterVersion()
     */
    void syncMap(String wanReplicationName,
                 String targetGroupName,
                 String mapName,
                 long fromTimestamp,
                 long toTimestamp);

    /**
     * Initiate WAN sync for all maps.
     * This method may sync only some entries which have been updated in a
     * certain time interval. The interval is defined by the {@code fromTimestamp}
     * and {@code toTimestamp} parameters. These parameters represent the
     * timestamp as defined by the {@link Clock#currentTimeMillis()} method.
     * <p>
     * NOTE: This method will throw a {@link UnsupportedOperationException} if
     * invoked on OS or if the cluster version is less than 3.9.
     * <p>
     * If the cluster contains members with versions less than 3.9.5, this
     * invocation will pass but the WAN sync will fail.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the group name on the target cluster
     * @param fromTimestamp      the earliest time since when the entry must
     *                           have been changed
     * @param toTimestamp        the latest time until when the entry must have
     *                           been changed
     * @throws UnsupportedOperationException if the operation is not supported (not EE)
     * @throws InvalidConfigurationException if there is no WAN replication
     *                                       config for {@code wanReplicationName}
     * @throws SyncFailedException           if there is a sync request in progress
     * @see Clock#currentTimeMillis()
     * @see ClusterService#getClusterVersion()
     */
    void syncAllMaps(String wanReplicationName,
                     String targetGroupName,
                     long fromTimestamp,
                     long toTimestamp);

    /**
     * Clears WAN replication queues of the given wanReplicationName for the given target.
     *
     * @param wanReplicationName the name of the wan replication config
     * @param targetGroupName    the target cluster group name
     */
    void clearQueues(String wanReplicationName, String targetGroupName);

    /**
     * Adds a new {@link WanReplicationConfig} to this member and creates the {@link WanReplicationPublisher}s specified
     * in the config.
     */
    void addWanReplicationConfig(WanReplicationConfig wanConfig);

    /**
     * Returns current status of WAN sync operation
     */
    WanSyncState getWanSyncState();

    /**
     * Returns a counter of received and processed WAN replication events.
     */
    WanEventCounter getReceivedEventCounter(String serviceName);

    /**
     * Returns a counter of sent and processed WAN replication events.
     */
    WanEventCounter getSentEventCounter(String serviceName);

    /**
     * Removes the WAN event counters for the given {@code dataStructureName}.
     */
    void removeWanEventCounters(String serviceName, String dataStructureName);
}
