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

import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;

/**
 * This interface offers the implementation of different kinds of replication techniques like
 * TCP, UDP or maybe even an JMS based service
 */
public interface WanReplicationPublisher {

    /**
     * Publish the {@code eventObject} WAN replication event. The event may be dropped if queue capacity has been reached.
     *
     * @param serviceName the service publishing the event
     * @param eventObject the replication event
     */
    void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject);

    /**
     * Publish the {@code eventObject} WAN replication event backup. The event may be dropped if queue capacity has been reached.
     *
     * @param serviceName the service publishing the event
     * @param eventObject the replication backup event
     */
    void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject);

    /**
     * Publishes the {@code wanReplicationEvent} on this publisher. This can be used to forward received events
     * on the target cluster.
     *
     * @param wanReplicationEvent the WAN event to publish
     */
    void publishReplicationEvent(WanReplicationEvent wanReplicationEvent);

    /**
     * Checks the size of the WAN replication queue and throws an
     * exception if it has been reached or crossed.
     *
     * @throws WANReplicationQueueFullException if queue capacity has been reached and
     *                                          {@link WanPublisherConfig#getQueueFullBehavior()} is
     *                                          set to {@link WANQueueFullBehavior#THROW_EXCEPTION}
     */
    void checkWanReplicationQueues();

}
