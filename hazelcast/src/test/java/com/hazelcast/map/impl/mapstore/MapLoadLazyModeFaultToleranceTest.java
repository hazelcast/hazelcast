/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.IMap;
import org.junit.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class MapLoadLazyModeFaultToleranceTest extends MapLoaderFaultToleranceBaseTest {

    @Override
    protected MapStoreConfig.InitialLoadMode getInitialLoadMode() {
        return MapStoreConfig.InitialLoadMode.LAZY;
    }

    @Test
    public void coordinatorShutdown_initialLoadAll_additionalFullLoadTriggered() throws InterruptedException {
        assumeGracefulShutdownOrLoadKeysScenario();
        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        performTest(
            () -> map.loadAll(true),
            instances[loadCoordinatorIndex],
            instances[requestNodeIndex],
            2,
            MAP_SIZE
        );
    }

    @Test
    public void coordinatorShutdown_notInitialLoadAll_additionalFullLoadTriggered() throws InterruptedException {
        assumeGracefulShutdown();

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        // initial load
        resumeLoading();
        map.loadAll(true);
        assertThat(map.size()).isEqualTo(MAP_SIZE);

        // second load
        var newSize = MAP_SIZE * 2;
        mapLoader.reset(new SimpleMapLoader(newSize, false));
        pauseLoading();

        performTest(
            () -> map.loadAll(true),
            instances[loadCoordinatorIndex],
            instances[requestNodeIndex],
            2,
            newSize
        );
    }

    @Test
    public void coordinatorShutdown_initialLoadBySomeMapOperation_additionalFullLoadTriggered() throws InterruptedException {
        assumeGracefulShutdownOrLoadKeysScenario();
        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        performTest(
            () -> map.get(1),
            instances[loadCoordinatorIndex],
            instances[requestNodeIndex],
            2,
            MAP_SIZE
        );
    }

    @Test
    public void coordinatorShutdown_loadAllSubset_additionalFullLoadTriggered() throws InterruptedException {
        assumeGracefulShutdownOrLoadKeysScenario();

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        performTest(
            () -> map.loadAll(Set.of(1), true),
            instances[loadCoordinatorIndex],
            instances[requestNodeIndex],
            2,
            MAP_SIZE
        );
    }

    @Test
    public void notCoordinatorShutdown_initialLoadAll_noAdditionalFullLoadTriggered() throws InterruptedException {
        assumeLoadKeysScenario();

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;
        var shuttingDownNodeIndex = (loadCoordinatorIndex + 2) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        performTest(
            () -> map.loadAll(true),
            instances[shuttingDownNodeIndex],
            instances[requestNodeIndex],
            1,
            MAP_SIZE
        );
    }

    @Test
    public void notCoordinatorShutdown_notInitialLoadAll_noAdditionalFullLoadTriggered() throws InterruptedException {
        assumeLoadKeysScenario();

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;
        var shuttingDownNodeIndex = (loadCoordinatorIndex + 2) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        // initial load
        resumeLoading();
        map.loadAll(true);
        assertThat(map.size()).isEqualTo(MAP_SIZE);

        // second load
        var newSize = MAP_SIZE * 2;
        mapLoader.reset(new SimpleMapLoader(newSize, false));
        pauseLoading();

        performTest(
            () -> map.loadAll(true),
            instances[shuttingDownNodeIndex],
            instances[requestNodeIndex],
            1,
            newSize
        );
    }

    @Test
    public void notCoordinatorShutdown_initialLoadBySomeMapOperation_noAdditionalFullLoadTriggered() throws InterruptedException {
        assumeLoadKeysScenario();

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;
        var shuttingDownNodeIndex = (loadCoordinatorIndex + 2) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        performTest(
            () -> map.get(1),
            instances[shuttingDownNodeIndex],
            instances[requestNodeIndex],
            1,
            MAP_SIZE
        );
    }

    @Test
    public void notCoordinatorShutdown_loadAllSubset_additionalFullLoadTriggered() throws InterruptedException {
        assumeLoadKeysScenario();

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;
        var shuttingDownNodeIndex = (loadCoordinatorIndex + 2) % CLUSTER_SIZE;

        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);

        performTest(
            () -> map.loadAll(Set.of(1), true),
            instances[shuttingDownNodeIndex],
            instances[requestNodeIndex],
            1,
            MAP_SIZE
        );
    }

}
