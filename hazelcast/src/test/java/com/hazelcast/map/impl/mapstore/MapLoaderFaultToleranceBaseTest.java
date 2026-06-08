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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.map.impl.mapstore.MapLoaderFaultToleranceBaseTest.FailureStage.LOAD_KEYS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

/*
 * Verifies that if one of the nodes is shut down/terminated during a map load,
 * no exception is thrown and the load completes successfully.
 *
 * Covers two failure stages:
 *   - LOAD_KEYS:   node dies while the coordinator is loading keys
 *   - LOAD_VALUES: node dies while the coordinator is loading values
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
public abstract class MapLoaderFaultToleranceBaseTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = "map";

    protected static final int MAP_SIZE = 1_000;
    protected static final int CLUSTER_SIZE = 3;
    protected static final int PARTITION_COUNT = 11;

    protected HazelcastInstance[] instances;
    protected ControlableMapLoader<Integer, Integer> mapLoader;

    public enum FailureStage {
        LOAD_KEYS,
        LOAD_VALUES
    }

    @Parameterized.Parameter()
    public boolean gracefulShutdown;

    @Parameterized.Parameter(1)
    public FailureStage failureStage;

    @Parameterized.Parameter(2)
    public int backupCount;

    @Parameterized.Parameters(name = "gracefulShutdown={0}, failureStage={1}, backupCount={2}")
    public static List<Object[]> parameters() {
        return cartesianProduct(
            List.of(true, false),
            List.of(FailureStage.values()),
            List.of(0, 1)
        );
    }

    @Before
    public void setup() {
        mapLoader = new ControlableMapLoader<>(new SimpleMapLoader(MAP_SIZE, false));

        if (failureStage == LOAD_KEYS) {
            mapLoader.pauseKeyLoading();
        } else {
            mapLoader.pauseValueLoading(PARTITION_COUNT);
        }

        Config config = getConfig();
        instances = createHazelcastInstances(config, CLUSTER_SIZE);
    }

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
            .setEnabled(true)
            .setImplementation(mapLoader)
            .setInitialLoadMode(getInitialLoadMode());

        config.addMapConfig(new MapConfig(MAP_NAME).setBackupCount(backupCount).setMapStoreConfig(mapStoreConfig));
        return config;
    }

    protected abstract MapStoreConfig.InitialLoadMode getInitialLoadMode();

    // The master does not currently provide fault tolerance for some cases where the
    // map loader fails while values are being loaded.
    // Add a test for value load scenarios after:
    // https://github.com/hazelcast/hazelcast-mono/pull/4888
    protected void assumeLoadKeysScenario() {
        assumeTrue(failureStage == LOAD_KEYS);
    }

    // The master does not currently provide fault tolerance for some cases of ungraceful shutdowns.
    // Add a test for ungraceful shutdown scenarios after:
    // https://github.com/hazelcast/hazelcast-mono/pull/4888
    protected void assumeGracefulShutdownOrLoadKeysScenario() {
        assumeTrue(failureStage == LOAD_KEYS || gracefulShutdown);
    }

    // The master does not currently provide fault tolerance for some cases of ungraceful shutdowns.
    // Add a test for ungraceful shutdown scenarios after:
    // https://github.com/hazelcast/hazelcast-mono/pull/4888
    protected void assumeGracefulShutdown() {
        assumeTrue(gracefulShutdown);
    }

    /**
     * Starts the load task asynchronously, waits until the loader reaches its
     * pause point, shuts down {@code shutdownInstance}, then unblocks the loader
     * and waits for the task to complete.
     */
    protected void performTest(
        Runnable taskThatTriggerLoad,
        HazelcastInstance shutdownInstance,
        HazelcastInstance stableInstance,
        int expectedFullLoadCount,
        int expectedMapSize
    ) throws InterruptedException {
        resetLoader();

        CompletableFuture<Void> task = CompletableFuture.runAsync(taskThatTriggerLoad);

        awaitLoadPaused();
        sleepSeconds(5);
        mapLoader.assertFullLoadCountEquals(1);

        stopInstanceAndWaitSafeState(shutdownInstance, stableInstance);

        resumeLoading();
        task.join();

        mapLoader.assertFullLoadCountEquals(expectedFullLoadCount);

        IMap<Integer, Integer> map = stableInstance.getMap(MAP_NAME);
        assertThat(map.size()).isEqualTo(expectedMapSize);

    }

    protected void pauseLoading() {
        if (failureStage == LOAD_KEYS) {
            mapLoader.pauseKeyLoading();
        } else {
            mapLoader.pauseValueLoading(PARTITION_COUNT);
        }
    }

    protected void resumeLoading() {
        if (failureStage == LOAD_KEYS) {
            mapLoader.resumeKeyLoading();
        } else {
            mapLoader.resumeValueLoading();
        }
    }

    protected void awaitLoadPaused() throws InterruptedException {
        if (failureStage == LOAD_KEYS) {
            mapLoader.awaitKeyLoadPaused();
        } else {
            mapLoader.awaitValueLoadPaused();
        }
    }

    protected void resetLoader() {
        if (failureStage == LOAD_KEYS) {
            mapLoader.reset();
        } else {
            mapLoader.reset(1, PARTITION_COUNT);
        }
    }

    protected int getLoadCoordinatorIndex() {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);
        var partition = nodeEngine.getPartitionService().getPartitionId(MAP_NAME);
        var address = nodeEngine.getPartitionService().getPartitionOwner(partition);
        for (int i = 0; i < instances.length; i++) {
            if (address.equals(instances[i].getCluster().getLocalMember().getAddress())) {
                return i;
            }
        }
        throw new RuntimeException("Load coordinator not found among cluster members");
    }

    protected void stopInstanceAndWaitSafeState(HazelcastInstance stopInstance, HazelcastInstance stableInstance) {
        System.out.println("Stopping instance: " + stopInstance.getCluster().getLocalMember().getAddress());
        if (gracefulShutdown) {
            stopInstance.shutdown();
        } else {
            stopInstance.getLifecycleService().terminate();
        }
        assertClusterSizeEventually(CLUSTER_SIZE - 1, stableInstance);
    }
}
