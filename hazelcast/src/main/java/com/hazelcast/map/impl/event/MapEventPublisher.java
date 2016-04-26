/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

/**
 * Helper methods for publishing events.
 *
 * @see MapEventPublisherImpl
 */
public interface MapEventPublisher {

    void publishWanReplicationUpdate(String mapName, EntryView entryView);

    void publishWanReplicationRemove(String mapName, Data key, long removeTime);

    void publishWanReplicationUpdateBackup(String mapName, EntryView entryView);

    void publishWanReplicationRemoveBackup(String mapName, Data key, long removeTime);

    void publishMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected);

    void publishEvent(Address caller, String mapName, EntryEventType eventType, Data dataKey, Object dataOldValue,
                      Object dataValue);

    void publishEvent(Address caller, String mapName, EntryEventType eventType,
                      Data dataKey, Object dataOldValue, Object dataValue, Object dataMergingValue);

    void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId);

    /**
     * Only gives a hint which indicates that a map-wide operation has just been executed on this partition.
     * This method should not publish an event.
     * <p/>
     * Currently a map event is published by the end which calls map#clear or map#evictAll and there is not
     * any order guarantee between events fired after map#put and map#clear, as a result of that, we may clear
     * a put after a map#clear, to tackle with that kind of possible anomalies, this hint may be used under
     * some conditions internally.
     */
    void hintMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected, int partitionId);

    boolean hasEventListener(String mapName);
}
