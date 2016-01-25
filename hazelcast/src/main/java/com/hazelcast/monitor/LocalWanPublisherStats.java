/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
     * Returns if the wan replication on this member is paused
     *
     * @return true the wan replication on this member is paused
     */
    boolean isPaused();
}
