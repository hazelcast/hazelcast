package com.hazelcast.map;

import com.hazelcast.core.EntryListener;
import com.hazelcast.spi.EventFilter;

public interface MapServiceContextEventListenerSupport {

    String addLocalEventListener(EntryListener entryListener, String mapName);

    String addLocalEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName);

    String addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName);

    boolean removeEventListener(String mapName, String registrationId);
}
