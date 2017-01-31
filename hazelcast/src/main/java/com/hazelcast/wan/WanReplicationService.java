/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.StatisticsAwareService;

/**
 * This is the WAN replications service API core interface. The WanReplicationService needs to
 * be capable of creating the actual {@link com.hazelcast.wan.WanReplicationPublisher} instances
 * to replicate values to other clusters over the wide area network, so it has to deal with long
 * delays, slow uploads and higher latencies.
 */
public interface WanReplicationService
        extends CoreService, StatisticsAwareService {

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
     * @param name name of WAN replication configuration
     * @param targetGroupName name of wan target cluster config
     */
    void pause(String name, String targetGroupName);

    /**
     * Resumes wan replication to target group for the called node.
     *
     * @param name name of WAN replication configuration
     * @param targetGroupName name of wan target cluster config
     */
    void resume(String name, String targetGroupName);

    void checkWanReplicationQueues(String name);

    void syncMap(String wanReplicationName, String targetGroupName, String mapName);

    void syncAllMaps(String wanReplicationName, String targetGroupName);

    /**
     * Clears WAN replication queues of the given wanReplicationName for the given target.
     *
     * @param wanReplicationName
     * @param targetGroupName
     */
    void clearQueues(String wanReplicationName, String targetGroupName);

    /**
     * Adds a new {@link WanReplicationConfig} to all members.
     */
    void addWanReplicationConfig(WanReplicationConfig wanConfig);

    /**
     * Returns current status of WAN sync operation
     */
    WanSyncState getWanSyncState();
}
