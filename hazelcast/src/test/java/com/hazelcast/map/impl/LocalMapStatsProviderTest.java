package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.monitor.impl.PartitionIndexesStats;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.impl.DummyInternalPartition;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
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

import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LocalMapStatsProviderTest {

    @Test
    public void nearCacheUsesNativeMemory() {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        Properties props = new Properties();
        props.put(GroupProperty.MAP_LOAD_CHUNK_SIZE, 22);
        doReturn(new HazelcastProperties(props)).when(nodeEngine).getProperties();
        MapServiceContext serviceContext = mock(MapServiceContext.class);
        doReturn(nodeEngine).when(serviceContext).getNodeEngine();
        ProxyService proxyService = mock(ProxyService.class);
        doReturn(emptyList()).when(proxyService).getDistributedObjectNames("hz:impl:serviceContext");
        doReturn(proxyService).when(nodeEngine).getProxyService();
        doReturn(mock(ClusterService.class)).when(nodeEngine).getClusterService();

        MapService mapService = mock(MapService.class);
        doReturn(serviceContext).when(mapService).getMapServiceContext();
        MapContainer mapContainer = mock(MapContainer.class);
        MapConfig mapConfig = new MapConfig();
        mapConfig.setNearCacheConfig(new NearCacheConfig());
        mapConfig.getNearCacheConfig().setInMemoryFormat(InMemoryFormat.NATIVE);
        doReturn(mapConfig).when(mapContainer).getMapConfig();
        doReturn(mapContainer).when(serviceContext).getMapContainer("asd");
        Indexes indexes = mock(Indexes.class);
        doReturn(indexes).when(mapContainer).createIndexes(false);
        doReturn(new PartitionIndexesStats()).when(indexes).getIndexesStats();
        doReturn(new InternalIndex[]{}).when(indexes).getIndexes();

        RecordStore mockRecordStore = mock(RecordStore.class);
        doReturn("asd").when(mockRecordStore).getName();
        doReturn(mapContainer).when(mockRecordStore).getMapContainer();
        doReturn(mockRecordStore).when(serviceContext).createRecordStore(eq(mapContainer), eq(0), any());

        IPartitionService partitionService = mock(IPartitionService.class);
        doReturn(partitionService).when(nodeEngine).getPartitionService();
        doReturn(new DummyInternalPartition(new PartitionReplica[]{}, 0)).when(partitionService).getPartition(0, false);
        doReturn(new DummyInternalPartition(new PartitionReplica[]{}, 0)).when(partitionService).getPartition(0);

        PartitionContainer partitionContainer = new PartitionContainer(mapService, 0);
        partitionContainer.getRecordStore("asd", false);
        doReturn(new PartitionContainer[]{partitionContainer}).when(serviceContext).getPartitionContainers();


        doReturn(mock(MapNearCacheManager.class)).when(serviceContext).getMapNearCacheManager();

        LocalMapStatsProvider provider = new LocalMapStatsProvider(serviceContext);
        Map<String, LocalMapStats> actual = provider.createAllLocalMapStats();

        System.out.println(actual);
    }

}
