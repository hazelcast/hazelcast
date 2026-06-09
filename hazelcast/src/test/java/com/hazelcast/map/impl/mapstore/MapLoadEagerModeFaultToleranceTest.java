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

import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.map.IMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

public class MapLoadEagerModeFaultToleranceTest extends MapLoaderFaultToleranceBaseTest {

    @Override
    protected InitialLoadMode getInitialLoadMode() {
        return InitialLoadMode.EAGER;
    }

    @Test
    public void coordinatorShutdown_initialLoadAll_additionalFullLoadTriggered() throws InterruptedException {
        assumeTrue(gracefulShutdown || failureStage == FailureStage.LOAD_KEYS || backupCount == 0);

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;

        performTest(
            () -> instances[requestNodeIndex].getMap(MAP_NAME),
            instances[loadCoordinatorIndex],
            instances[requestNodeIndex],
            2,
            MAP_SIZE
        );
    }

    @Test
    public void coordinatorShutdown_notInitialLoadAll_additionalFullLoadTriggered() throws InterruptedException {
        assumeTrue(gracefulShutdown || backupCount == 0);

        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;

        // initial load
        resumeLoading();
        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);
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
    public void notCoordinatorShutdown_initialLoadAll_noAdditionalFullLoadTriggered() throws InterruptedException {
        assumeLoadKeysScenario();
        var loadCoordinatorIndex = getLoadCoordinatorIndex();
        var requestNodeIndex = (loadCoordinatorIndex + 1) % CLUSTER_SIZE;
        var shuttingDownNodeIndex = (loadCoordinatorIndex + 2) % CLUSTER_SIZE;

        performTest(
            () -> instances[requestNodeIndex].getMap(MAP_NAME),
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

        // initial load
        resumeLoading();
        IMap<Integer, Integer> map = instances[requestNodeIndex].getMap(MAP_NAME);
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
}
