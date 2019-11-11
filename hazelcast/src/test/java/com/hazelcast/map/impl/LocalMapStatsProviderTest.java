package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.monitor.impl.PartitionIndexesStats;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.impl.DummyInternalPartition;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LocalMapStatsProviderTest {

    private static final String MAP_NAME = "myMap";
    private static final int PARTITION_ID = 0;

    @Test
    public void nearCache_withoutStats_UsesNativeMemory() {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        Properties props = new Properties();
        props.put(GroupProperty.MAP_LOAD_CHUNK_SIZE, 22);
        doReturn(new HazelcastProperties(props)).when(nodeEngine).getProperties();
        MapServiceContext serviceContext = mock(MapServiceContext.class);
        doReturn(nodeEngine).when(serviceContext).getNodeEngine();
        ProxyService proxyService = mock(ProxyService.class);
        doReturn(proxyService).when(nodeEngine).getProxyService();
        doReturn(mock(ClusterService.class)).when(nodeEngine).getClusterService();

        MapService mapService = mock(MapService.class);
        doReturn(serviceContext).when(mapService).getMapServiceContext();
        MapContainer mapContainer = mock(MapContainer.class);

        MapConfig mapConfig = new MapConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        doReturn(mapConfig).when(mapContainer).getMapConfig();
        doReturn(mapContainer).when(serviceContext).getMapContainer(MAP_NAME);
        doReturn(new PartitionContainer[]{}).when(serviceContext).getPartitionContainers();
        doReturn(singletonList(MAP_NAME)).when(proxyService).getDistributedObjectNames("hz:impl:mapService");

        doReturn(mock(MapNearCacheManager.class)).when(serviceContext).getMapNearCacheManager();

        LocalMapStatsProvider provider = new LocalMapStatsProvider(serviceContext);
        Map<String, LocalMapStats> actual = provider.createAllLocalMapStats();

        assertTrue(actual.get(MAP_NAME).getNearCacheStats().isNativeMemoryUsed());
    }

    private void setupPartitionContainer(NodeEngine nodeEngine, MapServiceContext serviceContext, MapService mapService,
                                         MapContainer mapContainer, String mapName) {
        Indexes indexes = mock(Indexes.class);
        doReturn(indexes).when(mapContainer).createIndexes(false);
        doReturn(new PartitionIndexesStats()).when(indexes).getIndexesStats();
        doReturn(new InternalIndex[]{}).when(indexes).getIndexes();

        RecordStore mockRecordStore = mock(RecordStore.class);
        doReturn(mapName).when(mockRecordStore).getName();
        doReturn(mapContainer).when(mockRecordStore).getMapContainer();
        doReturn(mockRecordStore).when(serviceContext).createRecordStore(eq(mapContainer), eq(PARTITION_ID), any());

        IPartitionService partitionService = mock(IPartitionService.class);
        doReturn(partitionService).when(nodeEngine).getPartitionService();
        doReturn(new DummyInternalPartition(new PartitionReplica[]{}, PARTITION_ID)).when(partitionService).getPartition(PARTITION_ID, false);
        doReturn(new DummyInternalPartition(new PartitionReplica[]{}, PARTITION_ID)).when(partitionService).getPartition(PARTITION_ID);

        PartitionContainer partitionContainer = new PartitionContainer(mapService, PARTITION_ID);
        partitionContainer.getRecordStore(mapName, false);
        doReturn(new PartitionContainer[]{partitionContainer}).when(serviceContext).getPartitionContainers();
    }

    @Test
    public void nearCache_WithStats_usesNativeMemory() {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        Properties props = new Properties();
        props.put(GroupProperty.MAP_LOAD_CHUNK_SIZE, 22);
        doReturn(new HazelcastProperties(props)).when(nodeEngine).getProperties();
        MapServiceContext serviceContext = mock(MapServiceContext.class);
        doReturn(nodeEngine).when(serviceContext).getNodeEngine();
        ProxyService proxyService = mock(ProxyService.class);
        doReturn(proxyService).when(nodeEngine).getProxyService();
        doReturn(mock(ClusterService.class)).when(nodeEngine).getClusterService();

        MapService mapService = mock(MapService.class);
        doReturn(serviceContext).when(mapService).getMapServiceContext();
        MapContainer mapContainer = mock(MapContainer.class);
        MapConfig mapConfig = new MapConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        doReturn(mapConfig).when(mapContainer).getMapConfig();

        NearCacheDataRecordStore<Object, Object> recordStore = new NearCacheDataRecordStore<>("default", nearCacheConfig,
                mock(SerializationService.class), getClass().getClassLoader());
        DefaultNearCache<Object, Object> nearCache = new DefaultNearCache<>("default", nearCacheConfig,
                recordStore, null, null, null, null);
        nearCache.initialize();

        MapNearCacheManager nearCacheManager = mock(MapNearCacheManager.class);
        doReturn(nearCacheManager).when(serviceContext).getMapNearCacheManager();
        doReturn(nearCache).when(nearCacheManager).getNearCache(MAP_NAME);

        doReturn(mapContainer).when(serviceContext).getMapContainer(MAP_NAME);
        setupPartitionContainer(nodeEngine, serviceContext, mapService, mapContainer, MAP_NAME);

        LocalMapStatsProvider provider = new LocalMapStatsProvider(serviceContext);
        provider.getLocalMapStatsImpl(MAP_NAME);
        Map<String, LocalMapStats> actual = provider.createAllLocalMapStats();

        assertTrue(actual.get(MAP_NAME).getNearCacheStats().isNativeMemoryUsed());
    }

}
