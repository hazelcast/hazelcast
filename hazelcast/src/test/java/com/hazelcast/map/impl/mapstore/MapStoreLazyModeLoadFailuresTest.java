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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.mapstore.MapStoreLazyModeLoadFailuresTest.ExceptionStage.LOAD_KEYS;
import static com.hazelcast.map.impl.mapstore.MapStoreLazyModeLoadFailuresTest.ExceptionStage.LOAD_VALUES;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests map store behaviour under key-load and value-load failures,
 * verifying that failed loads do not leave the map in a permanently broken state</li>
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MapStoreLazyModeLoadFailuresTest extends HazelcastTestSupport {

    public enum ExceptionStage {
        LOAD_KEYS,
        LOAD_VALUES
    }

    @Parameterized.Parameter()
    public ExceptionStage exceptionStage;

    @Parameterized.Parameters(name = "exceptionStage: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(LOAD_KEYS, LOAD_VALUES);
    }

    private static final int PARTITION_COUNT = 11;
    private static final int MAP_LOADER_SIZE = 10_000;
    private static final int CLUSTER_SIZE = 3;
    private static final String MAP_NAME_PREFIX = "mapWithFailingLoader_";

    private HazelcastInstance[] instances;
    private FailedLoader mapLoader;

    @Before
    public void setup() {
        mapLoader = new FailedLoader(MAP_LOADER_SIZE);

        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
            .setEnabled(true)
            .setInitialLoadMode(InitialLoadMode.LAZY)
            .setImplementation(mapLoader);
        config.getMapConfig(MAP_NAME_PREFIX + "*")
            .setMapStoreConfig(mapStoreConfig);

        instances = createHazelcastInstances(config, CLUSTER_SIZE);
        configMapLoaderFailure();
    }

    private String getMapName() {
        return getMapName(0);
    }

    private String getMapName(int partitionId) {
        return generateKeyForPartition(instances[0], MAP_NAME_PREFIX, partitionId);
    }

    private void configMapLoaderFailure() {
        mapLoader.reset();
        if (exceptionStage == LOAD_KEYS) {
            mapLoader.failOnKeyLoad();
        } else if (exceptionStage == LOAD_VALUES) {
            mapLoader.failOnValuesLoadAfter(1);
        } else {
            throw new IllegalStateException("Unknown exception stage: " + exceptionStage);
        }
    }

    private void assertMapIsAvailable(IMap<String, String> map) {
        range(0, PARTITION_COUNT).forEach(partition -> {
            String key = generateKeyForPartition(instances[0], partition);
            String value = "value" + key;
            map.put(key, value);
            assertThat(map.get(key)).isEqualTo(value);
        });
    }

    private void performLoadAllOperationAndCheck(IMap<String, String> map) {
        mapLoader.reset();
        map.clear();
        map.loadAll(true);
        assertThat(map.size()).isEqualTo(MAP_LOADER_SIZE);
    }

    @Test
    public void loadAllFailsSilently() {
        IMap<String, String> map = instances[0].getMap(getMapName());
        // trigger initial load
        assertThatNoException().isThrownBy(() -> map.loadAll(true));
        // not initial load
        assertThatNoException().isThrownBy(() -> map.loadAll(true));
    }

    @Test
    public void loadAllSubsetFailsSilently() {
        IMap<String, String> map = instances[0].getMap(getMapName());
        // trigger initial load
        assertThatNoException().isThrownBy(() -> map.loadAll(Set.of("1"), true));
        // not initial load
        assertThatNoException().isThrownBy(() -> map.loadAll(Set.of("1"), true));
    }

    @Test
    public void initialLoadFails_thenMapBecomesAvailable_andSubsequentLoadsSucceed() {
        IMap<String, String> map = instances[0].getMap(getMapName());
        map.loadAll(true);

        assertMapIsAvailable(map);
        performLoadAllOperationAndCheck(map);
    }

    @Test
    // Fix this test after:
    // https://github.com/hazelcast/hazelcast-mono/pull/4888
    public void initialLoadTriggeredByMapOperationFails_thenMapBecomesAvailable_andSubsequentLoadsSucceed() {
        IMap<String, String> map = instances[0].getMap(getMapName());
        try {
            map.get(generateKeyForPartition(instances[0], 0));
        } catch (RuntimeException e) {
            // This is a master inconsistency bug:
            // The operation may propagate the load exception or suppress it;
        }
        // This is a master inconsistency bug: after a failure, we may still receive
        // an exception from a previous load, but after several iterations the map
        // becomes available again.
        assertTrueEventually(() -> assertThatNoException().isThrownBy(() -> assertMapIsAvailable(map)));
        performLoadAllOperationAndCheck(map);
    }

    @Test
    public void initialLoadFailed_then_noFullLoadTriggersAgain() {
        IMap<String, String> map = instances[0].getMap(getMapName());
        range(0, PARTITION_COUNT).forEach(partition -> {
            try {
                map.get(generateKeyForPartition(instances[0], partition));
            } catch (RuntimeException e) {
                // This is a master inconsistency bug:
                // The operation may propagate the load exception or suppress it;
                // Fix this test after:
                // https://github.com/hazelcast/hazelcast-mono/pull/4888
            }
        });

        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void initialLoadFailedCoordinatorNodeFailed_backupDoesNotTriggerInitialLoad() {
        var coordinatorPartitionId = getPartitionId(instances[0]);
        IMap<String, String> map = instances[1].getMap(getMapName(coordinatorPartitionId));
        map.loadAll(true);
        mapLoader.assertFullLoadTriggeredOnce();
        mapLoader.reset();

        instances[0].getLifecycleService().terminate();
        waitAllForSafeState(instances);

        // try trigger initial load again
        assertThat(map.size()).isLessThan(MAP_LOADER_SIZE);

        mapLoader.assertNoFullLoadTriggered();
    }

    /**
     * A map loader that can be configured to fail during key or value loading.
     * This loader inherits all functionality from {@link ControlableMapLoader}.
     * Failures are triggered after the corresponding superclass method has been executed.
     */
    private static class FailedLoader extends ControlableMapLoader<String, String> {

        private volatile boolean failOnKeyLoad = false;

        /**
         * Remaining value-load calls before a failure is thrown.
         * {@code Integer.MAX_VALUE} means "never fail".
         */
        private volatile AtomicInteger remainingValueLoadsBeforeFailure =
            new AtomicInteger(Integer.MAX_VALUE);

        FailedLoader(int size) {
            super(new SimpleStringMapLoader(size), /* initialDelayMs= */ 0);
        }

        /**
         * Resets all failure flags and restores the loader to a healthy state.
         */
        public void reset() {
            super.reset(0);
            failOnKeyLoad = false;
            remainingValueLoadsBeforeFailure = new AtomicInteger(Integer.MAX_VALUE);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            var loadResult = super.loadAll(keys);
            if (remainingValueLoadsBeforeFailure.decrementAndGet() < 0) {
                throw new RuntimeException("Test failed to load values");
            }
            return loadResult;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            var keys = super.loadAllKeys();
            if (failOnKeyLoad) {
                throw new RuntimeException("Test failed to load keys");
            }
            return keys;
        }

        public void failOnKeyLoad() {
            this.failOnKeyLoad = true;
        }

        /**
         * Configures the loader to throw after {@code allowedCalls} successful
         * value-load calls.  Pass {@code 0} to fail on the very first call.
         */
        public void failOnValuesLoadAfter(int allowedCalls) {
            this.remainingValueLoadsBeforeFailure = new AtomicInteger(allowedCalls);
        }
    }
}
