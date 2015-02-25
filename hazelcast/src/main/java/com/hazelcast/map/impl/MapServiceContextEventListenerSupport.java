package com.hazelcast.map.impl;

import com.hazelcast.map.listener.MapListener;
import com.hazelcast.spi.EventFilter;

/**
 * Helper event listener methods for {@link MapServiceContext}.
 */
public interface MapServiceContextEventListenerSupport {

    String addLocalEventListener(MapListener mapListener, String mapName);

    String addLocalEventListener(MapListener mapListener, EventFilter eventFilter, String mapName);

    String addEventListener(MapListener mapListener, EventFilter eventFilter, String mapName);

    boolean removeEventListener(String mapName, String registrationId);

}
