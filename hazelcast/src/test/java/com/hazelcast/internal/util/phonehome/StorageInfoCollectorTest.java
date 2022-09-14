package com.hazelcast.internal.util.phonehome;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DATA_MEMORY_COST;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.HD_MEMORY_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.LICENSE_KEY_VERSION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_FREE_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_NATIVE_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIERED_STORAGE_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIERED_STORAGE_HELD;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIERED_STORAGE_LIMIT;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class})
public class StorageInfoCollectorTest {

    private final static String V6_ALL_FEATURES_ENABLED_LICENSE = "CUSTOM_LICENSE#40Nodes#" +
            "67vf8iUts1yEMdjbJgBZXWDFkI2RQuALCK0hHacOY4zP5q3pGNw17101001111510100190090011109091001019001491000100100";

    StorageInfoCollector storageInfoCollector;

    @Mock
    Node node;

    @Mock
    NodeEngineImpl nodeEngine;

    @Mock
    NodeExtension nodeExtension;

    @Mock
    MemoryStats memoryStats;

    @Mock
    MapService mapService;

    @Mock
    ReplicatedMapService replicatedMapService;

    @Mock
    BiConsumer<PhoneHomeMetrics, String> metricsConsumer;

    Config config;

    @Before
    public void setUp() throws Exception {
        storageInfoCollector = new StorageInfoCollector();

        when(node.getNodeExtension()).thenReturn(nodeExtension);
        when(node.getNodeEngine()).thenReturn(nodeEngine);
        when(node.getBuildInfo()).thenReturn(BuildInfoProvider.getBuildInfo());

        when(nodeExtension.getMemoryStats()).thenReturn(memoryStats);

        when(nodeEngine.getService(MapService.SERVICE_NAME)).thenReturn(mapService);
        when(nodeEngine.getService(ReplicatedMapService.SERVICE_NAME)).thenReturn(replicatedMapService);
        config = new Config().setLicenseKey(V6_ALL_FEATURES_ENABLED_LICENSE);
        when(nodeEngine.getConfig()).thenReturn(config);
    }

    @Test
    public void test_memory() {
        // given
        when(memoryStats.getUsedNative()).thenReturn(5L);
        when(memoryStats.getUsedHeap()).thenReturn(10L);
        when(memoryStats.getFreeHeap()).thenReturn(20L);
        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true));

        // when
        storageInfoCollector.forEachMetric(node, metricsConsumer);

        // then
        verify(metricsConsumer).accept(HD_MEMORY_ENABLED, "true");
        verify(metricsConsumer).accept(MEMORY_USED_NATIVE_SIZE, "5");
        verify(metricsConsumer).accept(MEMORY_USED_HEAP_SIZE, "10");
        verify(metricsConsumer).accept(MEMORY_FREE_HEAP_SIZE, "20");
        verify(metricsConsumer).accept(TIERED_STORAGE_ENABLED, "false");
        verify(metricsConsumer).accept(DATA_MEMORY_COST, "0");
        verifyNoMoreInteractions(metricsConsumer);
    }

    @Test
    public void test_tieredStorage() {
        // given
        config.setNativeMemoryConfig(new NativeMemoryConfig());
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTieredStoreConfig(new TieredStoreConfig().setEnabled(true));
        config.setMapConfigs(singletonMap("default", mapConfig));

        // when
        storageInfoCollector.forEachMetric(node, metricsConsumer);

        // then
        verify(metricsConsumer).accept(HD_MEMORY_ENABLED, "false");
        verify(metricsConsumer).accept(MEMORY_USED_HEAP_SIZE, "0");
        verify(metricsConsumer).accept(MEMORY_FREE_HEAP_SIZE, "0");
        verify(metricsConsumer).accept(TIERED_STORAGE_ENABLED, "true");
        verify(metricsConsumer).accept(TIERED_STORAGE_LIMIT, "0");
        verify(metricsConsumer).accept(TIERED_STORAGE_HELD, "0");
        verify(metricsConsumer).accept(LICENSE_KEY_VERSION, "V6");
        verify(metricsConsumer).accept(DATA_MEMORY_COST, "0");
        verifyNoMoreInteractions(metricsConsumer);
    }

    @Test
    public void test_dataMemoryCost() {
        // given
        config.setNativeMemoryConfig(new NativeMemoryConfig());

        LocalMapStats mapStats1 = mock(LocalMapStats.class);
        when(mapStats1.getOwnedEntryMemoryCost()).thenReturn(10L);
        LocalMapStats mapStats2 = mock(LocalMapStats.class);
        when(mapStats2.getOwnedEntryMemoryCost()).thenReturn(5L);
        when(mapService.getStats()).thenReturn(ImmutableMap.of("map1", mapStats1, "map2", mapStats2));

        LocalReplicatedMapStats replicatedMapStats1 = mock(LocalReplicatedMapStats.class);
        when(replicatedMapStats1.getOwnedEntryMemoryCost()).thenReturn(20L);
        LocalReplicatedMapStats replicatedMapStats2 = mock(LocalReplicatedMapStats.class);
        when(replicatedMapStats2.getOwnedEntryMemoryCost()).thenReturn(40L);
        when(replicatedMapService.getStats())
                .thenReturn(ImmutableMap.of("map1", replicatedMapStats1, "map2", replicatedMapStats2));

        // when
        storageInfoCollector.forEachMetric(node, metricsConsumer);

        // then
        verify(metricsConsumer).accept(HD_MEMORY_ENABLED, "false");
        verify(metricsConsumer).accept(MEMORY_USED_HEAP_SIZE, "0");
        verify(metricsConsumer).accept(MEMORY_FREE_HEAP_SIZE, "0");
        verify(metricsConsumer).accept(TIERED_STORAGE_ENABLED, "false");
        verify(metricsConsumer).accept(DATA_MEMORY_COST, "75");
        verifyNoMoreInteractions(metricsConsumer);
    }
}
