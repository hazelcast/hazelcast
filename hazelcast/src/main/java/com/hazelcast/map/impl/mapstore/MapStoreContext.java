package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

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

    void start();

    void stop();

    MapStoreManager getMapStoreManager();

    Map<Data, Object> getInitialKeys();

    MapStoreWrapper getMapStoreWrapper();

    boolean isWriteBehindMapStoreEnabled();

    SerializationService getSerializationService();

    ILogger getLogger(Class clazz);

    String getMapName();

    MapServiceContext getMapServiceContext();

    MapStoreConfig getMapStoreConfig();

    void waitInitialLoadFinish() throws Exception;
}
