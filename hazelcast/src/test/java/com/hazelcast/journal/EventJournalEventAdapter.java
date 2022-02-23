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

package com.hazelcast.journal;

import java.io.Serializable;

/**
 * Adapter interface for reading data-structure-specific event journal events
 *
 * @param <K>       the event key type
 * @param <V>       the event value type
 * @param <EJ_TYPE> the event type
 */
public interface EventJournalEventAdapter<K, V, EJ_TYPE> extends Serializable {

    /**
     * Returns the event key
     */
    K getKey(EJ_TYPE e);

    /**
     * Returns the event new value
     */
    V getNewValue(EJ_TYPE e);

    /**
     * Returns the event old value
     */
    V getOldValue(EJ_TYPE e);

    /**
     * Returns the unified event type
     */
    EventType getType(EJ_TYPE e);

    /**
     * Unified event type for all data structures. Data structure specific
     * events may have separate enums for event types.
     */
    enum EventType {
        ADDED, REMOVED, UPDATED, EVICTED, LOADED;
    }
}
