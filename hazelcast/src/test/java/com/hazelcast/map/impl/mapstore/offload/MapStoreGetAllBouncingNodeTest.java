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

package com.hazelcast.map.impl.mapstore.offload;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class})
public class MapStoreGetAllBouncingNodeTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "map-name";
    private static final Logger log = LoggerFactory.getLogger(MapStoreGetAllBouncingNodeTest.class);

    @Parameterized.Parameters(name = "offload: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Parameterized.Parameter
    public boolean offloadEnabled;

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(this::getConfig)
                    .driverType(BounceTestConfiguration.DriverType.MEMBER)
                    .clusterSize(3)
                    .driverCount(1)
                    .build();

    private static final int TEST_RUN_SECONDS = 20;

    @Test(timeout = 5 * 60 * 1000)
    public void stressReads() {
        final int keySpace = 1_000;

        IMap<Integer, String> map = bounceMemberRule.getSteadyMember().getMap(MAP_NAME);

        Runnable runnable = () -> {
            OpType[] values = OpType.values();
            OpType op = values[RandomPicker.getInt(values.length)];
            op.doOp(map, RandomPicker.getInt(2, keySpace));
        };

        bounceMemberRule.testRepeatedly(10, runnable, TEST_RUN_SECONDS);

        log.info("Map size after test {}", map.size());
    }

    @Test(timeout = 5 * 60 * 1000)
    public void stressGetAll() {
        final int keySpace = 1_000;

        IMap<Integer, String> map = bounceMemberRule.getSteadyMember().getMap(MAP_NAME);

        Runnable runnable = () -> OpType.GET_ALL.doOp(map, RandomPicker.getInt(2, keySpace));

        bounceMemberRule.testRepeatedly(10, runnable, TEST_RUN_SECONDS);

        log.info("Map size after test {}", map.size());
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig(MAP_NAME)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(getInMemoryFormat())
                .getMapStoreConfig()
                .setEnabled(true)
                .setOffload(offloadEnabled)
                .setImplementation(new MapStoreAdapter<Integer, String>() {

                    @Override
                    public String load(Integer key) {
                        sleepRandomMillis();
                        // simulate case when only even keys exist in the underlying store
                        return (key % 2 == 0) ? String.valueOf(key) : null;
                    }
                });
        return config;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    private static void sleepRandomMillis() {
        sleepMillis(RandomPicker.getInt(0, 3));
    }

    private enum OpType {

        GET {
            @Override
            void doOp(IMap<Integer, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    var key = ThreadLocalRandom.current().nextInt();
                    var value = map.get(key);
                    if (key % 2 == 0) {
                        assertThat(value).isNotNull();
                    } else {
                        assertThat(value).isNull();
                    }
                }
            }
        },

        GET_ALL {
            @Override
            void doOp(IMap<Integer, String> map, int keySpace) {
                Set<Integer> keys = new HashSet<>();
                int oddKeys = 0;
                for (int i = 0; i < keySpace; i++) {
                    var key = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
                    oddKeys += key % 2;
                    keys.add(key);
                }
                assertThat(map.getAll(keys)).hasSize(keySpace - oddKeys);
            }
        };

        abstract void doOp(IMap<Integer, String> map, int keySpace);
    }
}
