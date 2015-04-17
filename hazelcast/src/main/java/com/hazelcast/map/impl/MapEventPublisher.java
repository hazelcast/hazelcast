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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

/**
 * Helper methods for publishing events.
 *
 * @see MapEventPublisherSupport
 */
public interface MapEventPublisher {

    void publishWanReplicationUpdate(String mapName, EntryView entryView);

    void publishWanReplicationRemove(String mapName, Data key, long removeTime);

    void publishMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected);

    void publishEvent(Address caller, String mapName, EntryEventType eventType,
                      Data dataKey, Data dataOldValue, Data dataValue);

    void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean synthetic,
                      Data dataKey, Data dataOldValue, Data dataValue);

    void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean synthetic,
                      Data dataKey, Data dataOldValue, Data dataValue, Data dataMergingValue);

    void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId);

}
