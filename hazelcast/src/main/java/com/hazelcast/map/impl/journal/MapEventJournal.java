/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.journal;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.internal.journal.EventJournal;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

/**
 * The event journal is a container for events related to a data structure.
 * This interface provides methods for map event journals. This includes
 * events such as add, update, remove, evict and others. Each map and
 * partition has it's own event journal.
 * <p>
 * If a map is destroyed or the migrated, the related event journal will be destroyed or
 * migrated as well. In this sense, the event journal is co-located with the map partition
 * and it's replicas.
 * <p>
 * <b>NOTE :</b>
 * Map evictions are based on random samples which are then compared according to
 * an eviction policy. This is done separately on the partition owner and the backup replica
 * and can cause different entries to be evicted on the primary and backup replica.
 * Because of this, the record store can contain different entries and the event journal
 * can contain eviction/remove events for different entries on different replicas. This may
 * cause some issues if the partition owner crashes and the backup replica is promoted to
 * the partition owner. Readers of the event journal will then continue reading from the
 * promoted replica and may get update and remove events for entries which have already
 * been removed.
 *
 * @since 3.9
 */
public interface MapEventJournal extends EventJournal<InternalEventJournalMapEvent> {

    /**
     * Writes an {@link com.hazelcast.core.EntryEventType#UPDATED} to the event journal.
     * If there is no event journal configured for this map, the method will do nothing.
     * If an event is added to the event journal, all parked operations waiting for
     * new events on that journal will be unparked.
     *
     * @param journalConfig the event journal config for the map in which the event occurred
     * @param namespace     the map namespace
     * @param partitionId   the entry key partition
     * @param key           the entry key
     * @param oldValue      the old value
     * @param newValue      the new value
     */
    void writeUpdateEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                          Data key, Object oldValue, Object newValue);

    /**
     * Writes an {@link com.hazelcast.core.EntryEventType#ADDED} to the event journal.
     * If there is no event journal configured for this map, the method will do nothing.
     * If an event is added to the event journal, all parked operations waiting for
     * new events on that journal will be unparked.
     *
     * @param journalConfig the event journal config for the map in which the event occurred
     * @param namespace     the map namespace
     * @param partitionId   the entry key partition
     * @param key           the entry key
     * @param value         the entry value
     */
    void writeAddEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                       Data key, Object value);

    /**
     * Writes an {@link com.hazelcast.core.EntryEventType#REMOVED} to the event journal.
     * If there is no event journal configured for this map, the method will do nothing.
     * If an event is added to the event journal, all parked operations waiting for
     * new events on that journal will be unparked.
     *
     * @param journalConfig the event journal config for the map in which the event occurred
     * @param namespace     the map namespace
     * @param partitionId   the entry key partition
     * @param key           the entry key
     * @param value         the entry value
     */
    void writeRemoveEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId, Data key, Object value);

    /**
     * Writes an {@link com.hazelcast.core.EntryEventType#EVICTED} to the event journal.
     * If there is no event journal configured for this map, the method will do nothing.
     * If an event is added to the event journal, all parked operations waiting for
     * new events on that journal will be unparked.
     *
     * @param journalConfig the event journal config for the map in which the event occurred
     * @param namespace     the map namespace
     * @param partitionId   the entry key partition
     * @param key           the entry key
     * @param value         the entry value
     */
    void writeEvictEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId, Data key, Object value);

    /**
     * Returns {@code true} if the object has a configured and enabled event journal.
     *
     * @param namespace the object namespace
     * @return {@code true} if the object has a configured and enabled event journal, {@code false} otherwise
     */
    boolean hasEventJournal(ObjectNamespace namespace);
}
