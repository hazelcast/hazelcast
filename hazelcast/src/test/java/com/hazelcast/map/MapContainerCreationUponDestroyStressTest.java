/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class MapContainerCreationUponDestroyStressTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 271;

    /**
     * Tests newly created recordStores after imap#destroy references same MapContainer instance.
     * <p>
     * Normally, we remove mapContainer in map#destroy operation. If we call map#put after map#destroy, a new mapContainer
     * instance is created and all newly created recordStores references that new mapContainer instance. That referenced
     * mapContainer instance should be the new-mapContainer-instance because we previously called map#destroy and removed
     * old mapContainer instance.
     * <p>
     * This test trying to be sure that after map#destroy those all newly created recordStore instances references same
     * single mapContainer instance by stressing mapContainer creation process with subsequent map#put and map#destroy operations.
     */
    @Test
    public void testNewRecordStores_sharesSameMapContainerInstance_afterMapDestroy() throws Exception {
        String mapName = "map";
        final IMap<Long, Long> map = getIMap(mapName);
        final AtomicBoolean stop = new AtomicBoolean(false);

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stop.get()) {
                    map.put(System.currentTimeMillis(), System.currentTimeMillis());
                }
            }
        });

        Thread destroyThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stop.get()) {
                    map.destroy();
                    sleepAtLeastMillis(100);
                }
            }
        });

        putThread.start();
        destroyThread.start();

        sleepSeconds(15);
        stop.set(true);

        putThread.join();
        destroyThread.join();

        // make a final map#put to create recordStores
        for (long i = 0; i < PARTITION_COUNT; i++) {
            map.put(i, i);
        }

        assertRecordStoresSharesSameMapContainerInstance(map);
    }

    private void assertRecordStoresSharesSameMapContainerInstance(IMap<Long, Long> map) {
        String mapName = map.getName();

        MapContainer expectedMapContainer = getMapContainer(map);

        for (int i = 0; i < PARTITION_COUNT; i++) {
            PartitionContainer partitionContainer = getMapServiceContext(map).getPartitionContainer(i);
            RecordStore recordStore = partitionContainer.getMaps().get(mapName);
            if (recordStore == null) {
                continue;
            }
            assertEquals(expectedMapContainer, recordStore.getMapContainer());
        }
    }

    private IMap<Long, Long> getIMap(String mapName) {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setSize(10000).setMaxSizePolicy(PER_NODE);
        HazelcastInstance node = createHazelcastInstance(config);

        return node.getMap(mapName);
    }

    private MapContainer getMapContainer(IMap map) {
        MapServiceContext mapServiceContext = getMapServiceContext(map);
        return mapServiceContext.getMapContainers().get(map.getName());
    }

    private MapServiceContext getMapServiceContext(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        RemoteService service = mapProxy.getService();
        MapService mapService = (MapService) service;
        return mapService.getMapServiceContext();
    }
}
