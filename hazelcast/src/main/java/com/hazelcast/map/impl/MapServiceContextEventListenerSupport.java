package com.hazelcast.map.impl;

import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.spi.EventFilter;

/**
 * Helper event listener methods for {@link MapServiceContext}.
 */
public interface MapServiceContextEventListenerSupport {

    String addLocalEventListener(Object mapListener, String mapName);

    String addLocalEventListener(Object mapListener, EventFilter eventFilter, String mapName);

    String addEventListener(Object mapListener, EventFilter eventFilter, String mapName);

    String addPartitionLostListener(MapPartitionLostListener listener, String mapName);

    boolean removeEventListener(String mapName, String registrationId);

    boolean removePartitionLostListener(String mapName, String registrationId);

}
