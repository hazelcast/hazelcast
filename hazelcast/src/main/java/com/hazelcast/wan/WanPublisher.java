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

package com.hazelcast.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;

/**
 * This interface offers the implementation of different kinds of replication
 * techniques like TCP, UDP or maybe even an JMS based service.
 * Implementations of this interface represent a replication target,
 * normally another Hazelcast cluster only reachable over a Wide Area
 * Network (WAN).
 * The publisher may implement {@link com.hazelcast.core.HazelcastInstanceAware}
 * if it needs a reference to the instance on which it is being run.
 *
 * @param <T> type of event data that the publisher will publish
 */
public interface WanPublisher<T> {
    /**
     * Initializes the publisher.
     *
     * @param wanReplicationConfig {@link WanReplicationConfig} instance
     * @param publisherConfig      {@link AbstractWanPublisherConfig} instance
     */
    void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig publisherConfig);

    /**
     * Closes the publisher and its internal connections and shuts down other internal states.
     * Signals the publisher to shut down and clean up its resources. The
     * method does not necessarily block until the publisher has shut down.
     */
    void shutdown();

    /**
     * Resets the publisher (e.g. before split-brain merge).
     */
    default void reset() {
    }

    /**
     * Performs pre-publication checks (e.g. enforcing invariants).
     * Invoked before {@link #publishReplicationEvent(WanEvent)}
     * and {@link #publishReplicationEventBackup(WanEvent)}.
     */
    void doPrepublicationChecks();

    /**
     * Publish the {@code eventObject} WAN replication event.
     *
     * @param eventObject the replication event
     */
    void publishReplicationEvent(WanEvent<T> eventObject);

    /**
     * Publish the {@code eventObject} WAN replication event backup.
     *
     * @param eventObject the replication backup event
     */
    void publishReplicationEventBackup(WanEvent<T> eventObject);
}
