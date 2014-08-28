package com.hazelcast.map;

import com.hazelcast.map.operation.PostJoinMapOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PostJoinAwareService;

import java.util.Map;

class MapPostJoinAwareService implements PostJoinAwareService {

    private final MapServiceContext mapServiceContext;

    public MapPostJoinAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public Operation getPostJoinOperation() {
        PostJoinMapOperation o = new PostJoinMapOperation();
        final Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        for (MapContainer mapContainer : mapContainers.values()) {
            o.addMapIndex(mapContainer);
            o.addMapInterceptors(mapContainer);
        }
        return o;
    }
}
