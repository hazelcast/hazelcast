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


package com.hazelcast.internal.monitor;

import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.json.internal.JsonSerializable;
import com.hazelcast.wan.WanEventCounters.DistributedObjectWanEventCounters;
import com.hazelcast.wan.impl.WanSyncStats;
import com.hazelcast.wan.impl.ConsistencyCheckResult;
import com.hazelcast.wan.impl.WanReplicationService;

import java.util.Map;

/**
 * Local WAN publisher statistics to be used by {@link MemberState}
 * implementations.
 */
public interface LocalWanPublisherStats extends JsonSerializable {

    /**
     * Returns if this member is connected to the Wan publisher
     *
     * @return if this member is connected to the Wan publisher
     */
    boolean isConnected();

    /**
     * Returns the total number of published events from this member
     *
     * @return number of total published events from this member
     */
    long getTotalPublishedEventCount();

    /**
     * Returns the total latency of published wan events from this member by millisecond.
     *
     * @return latency of outbound records from this member by millisecond.
     */
    long getTotalPublishLatency();

    /**
     * Returns the outbound queue size on this member
     *
     * @return outbound queue size on this member
     */
    int getOutboundQueueSize();

    /**
     * Returns the current {@link WanPublisherState} of this publisher.
     *
     * @see WanReplicationService#pause(String, String)
     * @see WanReplicationService#stop(String, String)
     */
    WanPublisherState getPublisherState();

    /**
     * Returns the counter for the successfully transfered map WAN events.
     */
    Map<String, DistributedObjectWanEventCounters> getSentMapEventCounter();

    /**
     * Returns the counter for the successfully transfered cache WAN events.
     */
    Map<String, DistributedObjectWanEventCounters> getSentCacheEventCounter();

    /**
     * Returns the last results of the consistency checks, mapped by map name.
     */
    Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults();

    /**
     * Returns the last synchronization statistics, mapped by map name.
     */
    Map<String, WanSyncStats> getLastSyncStats();
}
