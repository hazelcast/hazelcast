/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionContainerImplTest
        extends HazelcastTestSupport {

    private static final String MAP_NAME = "somemap";

    private PartitionContainerImpl container;
    private RecordStore recordStore;
    private final TestConcurrentHashMap<String, RecordStore> map = new TestConcurrentHashMap<>();

    @Before
    public void preparePartitionContainerMocksForConstructorAndGetRecord() {
        MapService mapService = new MapService();
        mapService.mapServiceContext = mock(MapServiceContext.class);

        NodeEngine engine = mock(NodeEngine.class);
        when(mapService.mapServiceContext.getNodeEngine()).thenReturn(engine);

        Config config = new Config();
        config.setMapConfigs(Map.of(MAP_NAME, new MapConfig()));
        when(engine.getConfig()).thenReturn(config);

        try (MockedStatic<MapUtil> mapUtils = Mockito.mockStatic(MapUtil.class)) {
            mapUtils.when(() -> MapUtil.createConcurrentHashMap(anyInt())).thenReturn(map);
            container = new PartitionContainerImpl(mapService, 0);
        }

        //mock everything for createRecordStore used in PartitionContainerImpl::getRecordStore
        MapContainer mapContainer = mock(MapContainer.class);
        when(mapService.mapServiceContext.getMapContainer(anyString())).thenReturn(mapContainer);
        MapStoreContext mapStoreContext = mock(MapStoreContext.class);
        when(mapContainer.getMapStoreContext()).thenReturn(mapStoreContext);
        when(mapStoreContext.isMapLoader()).thenReturn(false);

        recordStore = mock(RecordStore.class);
        when(mapService.mapServiceContext.createRecordStore(mapContainer, 0, null)).thenReturn(recordStore);

    }

    @Test
    public void getRecordStore_startLoadingInvoked_withDefaultConstructor() {
        RecordStore recordStoreActual = container.getRecordStore(MAP_NAME);
        assertEquals(recordStoreActual, recordStore);
        verify(recordStoreActual).startLoading();
        assertTrue(container.getMaps().containsKey(MAP_NAME));
        assertEquals(1, container.getAllRecordStores().size());
    }

    @Test
    public void getRecordStore_startLoadingNotInvoked_whenSkipLoadedProvided() {
        RecordStore recordStoreActual = container.getRecordStore(MAP_NAME, true);
        assertEquals(recordStoreActual, recordStore);
        verify(recordStoreActual, times(0)).startLoading();
        assertTrue(container.getMaps().containsKey(MAP_NAME));
        assertEquals(1, container.getAllRecordStores().size());
    }

    /*
        Expect to call get 3 times,  2 operations happends one by one will to trt to put to maps new RecordStore
        First operaion will try to get from map once before mutex and one after mutex, second operation only before mutex and get value
        so would not override recordStore in map.
     */
    @Test
    public void getRecordStore_sameMap_notOverrideStore() {
        container.getRecordStore(MAP_NAME);
        container.getRecordStore(MAP_NAME);
        assertEquals(3, map.getCount);
        assertEquals(1, map.putCount);
        assertTrue(container.getMaps().containsKey(MAP_NAME));
        assertEquals(1, container.getAllRecordStores().size());
    }

    /*
        Expect to call get at least 3 times, cause concurrently 2 threads will tru to put to maps new RecordStore
        and will read once per thread before mutex and after mutex (sometimes one tread will read twice before another will read even once)
        but we still want to write in a map only once
        not to override existing record store even in race conditions
     */
    @Test
    public void getRecordStore_concurrency_notOverrideStore()
            throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(2);
        Thread threadWithLoad = new Thread(() -> {
            container.getRecordStore(MAP_NAME);
            latch.countDown();
        });
        Thread threadWithoutLoad = new Thread(() -> {
            container.getRecordStore(MAP_NAME, true);
            latch.countDown();
        });
        threadWithoutLoad.start();
        threadWithLoad.start();
        latch.await();

        assertTrue(map.getCount >= 3);
        assertEquals(1, map.putCount);
        assertTrue(container.getMaps().containsKey(MAP_NAME));
        assertEquals(1, container.getAllRecordStores().size());
    }

    static class TestConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
        private int putCount = 0;
        private int getCount = 0;

        @Override
        public V put(K key, V value) {
            putCount++;
            return super.put(key, value);
        }

        @Override
        public V get(Object key) {
            getCount++;
            return super.get(key);
        }
    }

}
