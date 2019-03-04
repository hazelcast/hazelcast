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

package com.hazelcast.monitor;

import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.MXBeansDTO;

import java.util.Collection;
import java.util.Map;

/**
 * Local statistics for a Hazelcast member.
 */
public interface MemberState extends JsonSerializable {

    String getAddress();

    /**
     * Returns the UUID of this member.
     *
     * @return the UUID of this member.
     */
    String getUuid();

    /**
     * Returns the local CP member's UUID if this Hazelcast
     * member is part of the CP subsystem, returns null otherwise.
     *
     * @return local CP member's UUID if available, null otherwise
     */
    String getCpMemberUuid();

    Map<String, Long> getRuntimeProps();

    LocalMapStats getLocalMapStats(String mapName);

    LocalMultiMapStats getLocalMultiMapStats(String mapName);

    LocalQueueStats getLocalQueueStats(String queueName);

    LocalTopicStats getLocalTopicStats(String topicName);

    LocalTopicStats getReliableLocalTopicStats(String reliableTopicName);

    LocalPNCounterStats getLocalPNCounterStats(String pnCounterName);

    LocalReplicatedMapStats getLocalReplicatedMapStats(String replicatedMapName);

    LocalExecutorStats getLocalExecutorStats(String executorName);

    LocalCacheStats getLocalCacheStats(String cacheName);

    LocalWanStats getLocalWanStats(String schemeName);

    LocalFlakeIdGeneratorStats getLocalFlakeIdGeneratorStats(String flakeIdName);

    Collection<ClientEndPointDTO> getClients();

    MXBeansDTO getMXBeans();

    LocalMemoryStats getLocalMemoryStats();

    /**
     * Returns the local operation statistics.
     *
     * @return LocalOperationStats statistics
     */
    LocalOperationStats getOperationStats();

    MemberPartitionState getMemberPartitionState();

    NodeState getNodeState();

    HotRestartState getHotRestartState();

    ClusterHotRestartStatusDTO getClusterHotRestartStatus();

    WanSyncState getWanSyncState();
}
