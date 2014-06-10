package com.hazelcast.map.client;

import com.hazelcast.core.EntryEvent;

/**
 * Entry Listener for clients
 */
public interface ClientEntryListener<K, V> {

    public void handleEvent(EntryEvent<K, V> event);
}
