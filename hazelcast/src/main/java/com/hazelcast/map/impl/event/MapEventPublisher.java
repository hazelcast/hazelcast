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

package com.hazelcast.map.impl.event;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.wan.WanMapEntryView;

import javax.annotation.Nonnull;

/**
 * Helper methods for publishing events related to map
 * actions. The implementation may delegate to other parts
 * of the system, for instance the WAN or event subsystem.
 *
 * @see MapEventPublisherImpl
 */
public interface MapEventPublisher {

    /**
     * Notifies the WAN subsystem of a map update on a replica owner.
     *
     * @param mapName           the map name
     * @param entryView         the updated entry
     * @param hasLoadProvenance {@code true} to indicate the provenance of
     *                          update is a load from map-loader, otherwise
     *                          set {@code false}
     */
    void publishWanUpdate(@Nonnull String mapName,
                          @Nonnull WanMapEntryView<Object, Object> entryView,
                          boolean hasLoadProvenance);

    /**
     * Notifies the WAN subsystem of a map entry removal on a replica owner.
     *
     * @param mapName the map name
     * @param key     the key of the removed entry
     */
    void publishWanRemove(@Nonnull String mapName, @Nonnull Data key);

    void publishMapEvent(Address caller, String mapName,
                         EntryEventType eventType, int numberOfEntriesAffected);

    /**
     * Publish an event to the event subsystem.
     * Note: Exceptions during publications are caught and logged.
     *
     * @param caller    the address of the caller that caused the event
     * @param mapName   the map name
     * @param eventType the event type
     * @param dataKey   the key of the event map entry
     * @param oldValue  the old value of the map entry
     * @param dataValue the new value of the map entry
     */
    void publishEvent(Address caller, String mapName,
                      EntryEventType eventType, Data dataKey, Object oldValue, Object dataValue);

    /**
     * Publish an event to the event subsystem. This method
     * can be used for a merge event since it also accepts
     * the value which was used in the merge process.
     * Note: Exceptions during publications are caught and logged.
     *
     * @param caller           the address of the caller that caused the event
     * @param mapName          the map name
     * @param eventType        the event type
     * @param dataKey          the key of the event map entry
     * @param oldValue         the old value of the map entry
     * @param dataValue        the new value of the map entry
     * @param dataMergingValue the value used when performing a merge
     *                         operation in case of a {@link EntryEventType#MERGED} event.
     *                         This value together with the old value produced the new value.
     */
    void publishEvent(Address caller, String mapName, EntryEventType eventType,
                      Data dataKey, Object oldValue, Object dataValue, Object dataMergingValue);

    void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId);

    /**
     * Only gives a hint which indicates that a map-wide
     * operation has just been executed on this partition.
     * This method should not publish an event.
     * <p>
     * Currently a map event is published by the end which calls map#clear
     * or map#evictAll and there is not any order guarantee between events
     * fired after map#put and map#clear, as a result of that, we may
     * clear a put after a map#clear, to tackle with that kind of possible
     * anomalies, this hint may be used under some conditions internally.
     */
    void hintMapEvent(Address caller, String mapName, EntryEventType eventType,
                      int numberOfEntriesAffected, int partitionId);

    /**
     * Notifies {@link com.hazelcast.map.QueryCache} subscribers directly,
     * without publishing an event to other map listeners. This is
     * necessary in certain cases, such as when loading entries into a map.
     *
     * @param eventData the event to publish to query caches
     */
    void addEventToQueryCache(Object eventData);

    /**
     * Returns {@code true} if there is at least one listener
     * registered for the specified {@code mapName}.
     */
    boolean hasEventListener(String mapName);
}
