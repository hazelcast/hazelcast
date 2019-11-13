/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl;

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
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LocalMapStatsProviderTest {

    private static final String MAP_NAME = "myMap";
    private static final int PARTITION_ID = 0;

    private static final MapConfig MAP_CONFIG = new MapConfig();

    static {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE);
        MAP_CONFIG.setNearCacheConfig(nearCacheConfig);
    }

    private NodeEngine nodeEngine;

    @Before
    public void initNodeEngine() {
        nodeEngine = mock(NodeEngine.class);
        Properties props = new Properties();
        props.put(GroupProperty.MAP_LOAD_CHUNK_SIZE, 22);
        doReturn(new HazelcastProperties(props)).when(nodeEngine).getProperties();
        doReturn(mock(ClusterService.class)).when(nodeEngine).getClusterService();

        ProxyService proxyService = mock(ProxyService.class);
        doReturn(proxyService).when(nodeEngine).getProxyService();
        doReturn(singletonList(MAP_NAME)).when(proxyService).getDistributedObjectNames("hz:impl:mapService");
    }

    @Test
    public void nearCache_withoutStats_UsesNativeMemory() {
        MapServiceContext serviceContext = mock(MapServiceContext.class);
        doReturn(nodeEngine).when(serviceContext).getNodeEngine();

        initMapContainer(serviceContext);
        doReturn(new PartitionContainer[]{}).when(serviceContext).getPartitionContainers();

        LocalMapStatsProvider provider = new LocalMapStatsProvider(serviceContext);
        Map<String, LocalMapStats> actual = provider.createAllLocalMapStats();

        assertSame(NATIVE, actual.get(MAP_NAME).getNearCacheStats().getInMemoryFormat());
    }

    private MapContainer initMapContainer(MapServiceContext serviceContext) {
        MapContainer mapContainer = mock(MapContainer.class);
        doReturn(MAP_CONFIG).when(mapContainer).getMapConfig();
        doReturn(mapContainer).when(serviceContext).getMapContainer(MAP_NAME);
        return mapContainer;
    }

    private void setupPartitionContainer(MapServiceContext serviceContext,
                                         MapContainer mapContainer) {
        MapService mapService = mock(MapService.class);
        doReturn(serviceContext).when(mapService).getMapServiceContext();
        Indexes indexes = mock(Indexes.class);
        doReturn(indexes).when(mapContainer).createIndexes(false);
        doReturn(new PartitionIndexesStats()).when(indexes).getIndexesStats();
        doReturn(new InternalIndex[]{}).when(indexes).getIndexes();

        RecordStore mockRecordStore = mock(RecordStore.class);
        doReturn(MAP_NAME).when(mockRecordStore).getName();
        doReturn(mapContainer).when(mockRecordStore).getMapContainer();
        doReturn(mockRecordStore).when(serviceContext).createRecordStore(eq(mapContainer), eq(PARTITION_ID), any());

        IPartitionService partitionService = mock(IPartitionService.class);
        doReturn(partitionService).when(nodeEngine).getPartitionService();
        doReturn(new DummyInternalPartition(new PartitionReplica[]{}, PARTITION_ID)).when(partitionService).getPartition(PARTITION_ID, false);
        doReturn(new DummyInternalPartition(new PartitionReplica[]{}, PARTITION_ID)).when(partitionService).getPartition(PARTITION_ID);

        PartitionContainer partitionContainer = new PartitionContainer(mapService, PARTITION_ID);
        partitionContainer.getRecordStore(MAP_NAME, false);
        doReturn(new PartitionContainer[]{partitionContainer}).when(serviceContext).getPartitionContainers();
    }

    @Test
    public void nearCache_WithStats_usesNativeMemory() {
        MapServiceContext serviceContext = mock(MapServiceContext.class);
        doReturn(nodeEngine).when(serviceContext).getNodeEngine();

        NearCacheDataRecordStore<Object, Object> recordStore = new NearCacheDataRecordStore<>("default",
                MAP_CONFIG.getNearCacheConfig(), mock(SerializationService.class), getClass().getClassLoader());
        DefaultNearCache<Object, Object> nearCache = new DefaultNearCache<>("default", MAP_CONFIG.getNearCacheConfig(),
                recordStore, null, null, null, null);
        nearCache.initialize();

        MapNearCacheManager nearCacheManager = mock(MapNearCacheManager.class);
        doReturn(nearCacheManager).when(serviceContext).getMapNearCacheManager();
        doReturn(nearCache).when(nearCacheManager).getNearCache(MAP_NAME);

        setupPartitionContainer(serviceContext, initMapContainer(serviceContext));

        LocalMapStatsProvider provider = new LocalMapStatsProvider(serviceContext);
        provider.getLocalMapStatsImpl(MAP_NAME);
        Map<String, LocalMapStats> actual = provider.createAllLocalMapStats();

        assertSame(NATIVE, actual.get(MAP_NAME).getNearCacheStats().getInMemoryFormat());
    }

}
