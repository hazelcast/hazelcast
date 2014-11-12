package com.hazelcast.map.impl;

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Map;

/**
 * Defines remote service behavior of map service.
 *
 * @see MapService
 */
class MapRemoteService implements RemoteService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public MapRemoteService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public MapProxyImpl createDistributedObject(String name) {
        return new MapProxyImpl(name, mapServiceContext.getService(), nodeEngine);
    }

    @Override
    public void destroyDistributedObject(String name) {
        final Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        MapContainer mapContainer = mapContainers.remove(name);
        if (mapContainer != null) {
            if (mapContainer.isNearCacheEnabled()) {
                mapServiceContext.getNearCacheProvider().remove(name);
            }
            mapContainer.getMapStoreContext().getMapStoreManager().stop();
        }
        mapServiceContext.destroyMap(name);
        nodeEngine.getEventService().deregisterAllListeners(mapServiceContext.serviceName(), name);
    }

}
