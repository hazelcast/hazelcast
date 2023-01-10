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

import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.wan.WanPublisher;

/**
 * Methods exposed on WAN publishers for internal use.
 */
public interface InternalWanPublisher<T> extends WanPublisher<T> {

    /**
     * Releases all resources for the map with the given {@code mapName}.
     *
     * @param mapName the map name
     */
    void destroyMapData(String mapName);

    /**
     * Removes a {@code count} number of events pending replication and belonging
     * to the provided service, object and partition.
     * If the publisher does not store WAN events, this method is a no-op.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @param serviceName the service name of the WAN events should be removed
     * @param objectName  the object name of the WAN events should be removed
     * @param partitionId the partition ID of the WAN events should be removed
     * @param count       the number of events to remove
     */
    int removeWanEvents(int partitionId, String serviceName, String objectName, int count);

    /**
     * Publishes the {@code wanReplicationEvent} on this publisher. This can be
     * used to forward received events on the target cluster.
     *
     * @param wanEvent the WAN event to publish
     */
    void republishReplicationEvent(InternalWanEvent<T> wanEvent);

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
     * pauses WAN replication for its {@link WanPublisher} instance.
     * <p>
     * WAN events will still be offered to WAN event containers but they won't
     * be polled. This means that the containers might eventually fill up and start
     * dropping events.
     * <p>
     * Calling this method on already paused {@link WanPublisher}
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
     * Calling this method on already stopped {@link WanPublisher}
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
     * {@link WanPublisher} instance.
     * <p>
     * Calling this method on already running {@link WanPublisher}
     * instances will have no effect.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @see #pause()
     * @see #stop()
     */
    default void resume() {
    }

    /**
     * Gathers statistics of related {@link WanPublisher} instance.
     * This method will always return the same instance.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @return {@link LocalWanPublisherStats}
     */
    default LocalWanPublisherStats getStats() {
        return null;
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
