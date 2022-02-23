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

/**
 * Context for unified event journal tests.
 */
public class EventJournalTestContext<K, V, EJ_TYPE> {

    /**
     * Adapter for a specific data structure holding the data and writing to
     * the event journal. This structure must not have configured any expiration
     * or eviction.
     * In a scenario with Hazelcast client and member, this will be an adapter
     * of the client proxy.
     */
    public final EventJournalDataStructureAdapter<K, V, EJ_TYPE> dataAdapter;

    /**
     * Adapter for a specific data structure holding the data and writing to
     * the event journal. This structure must have configured expiration of
     * entries as it is used in tests testing expiration behaviour.
     * In a scenario with Hazelcast client and member, this will be an adapter
     * of the client proxy.
     */
    public final EventJournalDataStructureAdapter<K, V, EJ_TYPE> dataAdapterWithExpiration;

    /**
     * Data structure specific adapter of the event journal events.
     */
    public final EventJournalEventAdapter<K, V, EJ_TYPE> eventJournalAdapter;


    public EventJournalTestContext(EventJournalDataStructureAdapter<K, V, EJ_TYPE> dataAdapter,
                                   EventJournalDataStructureAdapter<K, V, EJ_TYPE> dataAdapterWithExpiration,
                                   EventJournalEventAdapter<K, V, EJ_TYPE> eventJournalAdapter) {
        this.dataAdapter = dataAdapter;
        this.dataAdapterWithExpiration = dataAdapterWithExpiration;
        this.eventJournalAdapter = eventJournalAdapter;
    }
}
