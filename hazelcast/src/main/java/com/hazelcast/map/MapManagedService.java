package com.hazelcast.map;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStoreInfo;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;

/**
 * Defines managed service behavior of map service.
 *
 * @see com.hazelcast.map.MapService
 */
class MapManagedService implements ManagedService {

    private final MapServiceContext mapServiceContext;

    MapManagedService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        mapServiceContext.initPartitionsContainers();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(mapServiceContext.serviceName(),
                    new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
                        public LockStoreInfo createNew(final ObjectNamespace key) {
                            final MapContainer mapContainer = mapServiceContext.getMapContainer(key.getObjectName());
                            return new LockStoreInfo() {
                                public int getBackupCount() {
                                    return mapContainer.getBackupCount();
                                }

                                public int getAsyncBackupCount() {
                                    return mapContainer.getAsyncBackupCount();
                                }
                            };
                        }
                    });
        }
        mapServiceContext.getExpirationManager().start();
    }

    @Override
    public void reset() {
        mapServiceContext.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            final MapServiceContext mapServiceContext = this.mapServiceContext;
            mapServiceContext.flushMaps();
            mapServiceContext.destroyMapStores();
            mapServiceContext.clearPartitions();
            mapServiceContext.getNearCacheProvider().clear();
            mapServiceContext.getMapContainers().clear();
        }
    }


}
