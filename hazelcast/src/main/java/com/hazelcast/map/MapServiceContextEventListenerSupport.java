package com.hazelcast.map;

import com.hazelcast.core.EntryListener;
import com.hazelcast.spi.EventFilter;

/**
 * Helper event listener methods for {@link com.hazelcast.map.MapServiceContext}.
 */
public interface MapServiceContextEventListenerSupport {

    String addLocalEventListener(EntryListener entryListener, String mapName);

    String addLocalEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName);

    String addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName);

    boolean removeEventListener(String mapName, String registrationId);
}
