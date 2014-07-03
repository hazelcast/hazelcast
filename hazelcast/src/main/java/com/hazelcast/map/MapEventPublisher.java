package com.hazelcast.map;

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

    boolean hasEventRegistration(String topic);
}
