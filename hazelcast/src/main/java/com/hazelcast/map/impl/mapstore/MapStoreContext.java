package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;

/**
 * A context which provides/initializes map store specific functionality.
 * <p/>
 * Specifically,
 * <p/>
 * <li>creates map store implementation from map store configuration.</li>
 * <li>creates map store manager according to write-behind or write-through store configuration</li>
 * <li>loads initial keys if a loader defined.</li>
 */
public interface MapStoreContext {

    MapStoreManager getMapStoreManager();

    Map<Data, Object> getInitialKeys();

    MapStoreWrapper getStore();

    void start();

    void stop();

    boolean isWriteBehindMapStoreEnabled();
}
