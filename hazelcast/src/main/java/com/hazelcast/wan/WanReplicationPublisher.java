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

package com.hazelcast.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.partition.PartitionReplicationEvent;

import java.util.Collection;
import java.util.Set;

/**
 * This interface offers the implementation of different kinds of replication
 * techniques like TCP, UDP or maybe even an JMS based service.
 * Implementations of this interface represent a replication target,
 * normally another Hazelcast cluster only reachable over a Wide Area
 * Network (WAN).
 * The publisher may implement {@link com.hazelcast.core.HazelcastInstanceAware}
 * if it needs a reference to the instance on which it is being run.
 *
 * @param <T> WAN event container type (used for replication and migration inside the
 *            cluster)
 */
public interface WanReplicationPublisher<T> {
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
     * NOTE: used only in Hazelcast Enterprise.
     */
    default void reset() {
    }

    /**
     * Performs pre-publication checks (e.g. enforcing invariants).
     * Invoked before {@link #publishReplicationEvent(WanReplicationEvent)}
     * and {@link #publishReplicationEventBackup(WanReplicationEvent)}.
     */
    void doPrepublicationChecks();

    /**
     * Publish the {@code eventObject} WAN replication event.
     *
     * @param eventObject the replication event
     */
    void publishReplicationEvent(WanReplicationEvent eventObject);

    /**
     * Publish the {@code eventObject} WAN replication event backup.
     *
     * @param eventObject the replication backup event
     */
    void publishReplicationEventBackup(WanReplicationEvent eventObject);

    /**
     * Publishes the {@code wanReplicationEvent} on this publisher. This can be
     * used to forward received events on the target cluster.
     *
     * @param wanReplicationEvent the WAN event to publish
     */
    void republishReplicationEvent(WanReplicationEvent wanReplicationEvent);

    /**
     * Publishes a WAN anti-entropy event. This method may also process the
     * event or trigger processing.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @param event the WAN anti-entropy event
     */
    default void publishAntiEntropyEvent(WanAntiEntropyEvent event) {
    }

    /**
     * Calls to this method will pause WAN event container polling. Effectively,
     * pauses WAN replication for its {@link WanReplicationPublisher} instance.
     * <p>
     * WAN events will still be offered to WAN event containers but they won't
     * be polled. This means that the containers might eventually fill up and start
     * dropping events.
     * <p>
     * Calling this method on already paused {@link WanReplicationPublisher}
     * instances will have no effect.
     * <p></p>
     * There is no synchronization with the thread polling the WAN event
     * containers and trasmitting the events to the target cluster. This means
     * that the containers may be polled even after this method returns.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @see #resume()
     * @see #stop()
     */
    default void pause() {
    }

    /**
     * Calls to this method will stop WAN replication. In addition to not polling
     * events as in the {@link #pause()} method, a publisher which is stopped
     * should not accept new events. This method will not remove existing events.
     * This means that once this method returns, there might still be some WAN
     * events in the containers but these events will not be replicated until
     * the publisher is resumed.
     * <p>
     * Calling this method on already stopped {@link WanReplicationPublisher}
     * instances will have no effect.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @see #resume()
     * @see #pause()
     */
    default void stop() {
    }

    /**
     * This method re-enables WAN event containers polling for a paused or stopped
     * {@link WanReplicationPublisher} instance.
     * <p>
     * Calling this method on already running {@link WanReplicationPublisher}
     * instances will have no effect.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @see #pause()
     * @see #stop()
     */
    default void resume() {
    }

    /**
     * Gathers statistics of related {@link WanReplicationPublisher} instance.
     * This method will always return the same instance.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @return {@link LocalWanPublisherStats}
     */
    default LocalWanPublisherStats getStats() {
        return null;
    }

    /**
     * Returns a container containing the WAN events for the given replication
     * {@code event} and {@code namespaces} to be replicated. The replication
     * here refers to the intra-cluster replication between members in a single
     * cluster and does not refer to WAN replication, e.g. between two clusters.
     * Invoked when migrating WAN replication data between members in a cluster.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @param event      the replication event
     * @param namespaces namespaces which will be replicated
     * @return the WAN event container
     * @see #processEventContainerReplicationData(int, Object)
     */
    default T prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                   Collection<ServiceNamespace> namespaces) {
        return null;
    }

    /**
     * Processes the WAN event container received through intra-cluster replication
     * or migration. This method may completely remove existing WAN events for
     * the given {@code partitionId} or it may append the given
     * {@code eventContainer} to the existing events.
     * Invoked when migrating WAN replication data between members in a cluster.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @param partitionId    partition ID which is being replicated or migrated
     * @param eventContainer the WAN event container
     * @see #prepareEventContainerReplicationData(PartitionReplicationEvent, Collection)
     */
    default void processEventContainerReplicationData(int partitionId, T eventContainer) {
    }

    /**
     * Collect the namespaces of all WAN event containers that should be replicated
     * by the replication event.
     * Invoked when migrating WAN replication data between members in a cluster.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    default void collectAllServiceNamespaces(PartitionReplicationEvent event,
                                             Set<ServiceNamespace> namespaces) {
    }

    /**
     * Removes all WAN events awaiting replication.
     * If the publisher does not store WAN events, this method is a no-op.
     * Invoked when clearing the WAN replication data, e.g. because of a REST call.
     * NOTE: used only in Hazelcast Enterprise.
     */
    default int removeWanEvents() {
        return 0;
    }
}
