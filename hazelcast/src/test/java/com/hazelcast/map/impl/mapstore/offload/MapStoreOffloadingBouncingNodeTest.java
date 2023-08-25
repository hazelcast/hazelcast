/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.map.IMapAccessors.getPendingOffloadedOpCount;
import static com.hazelcast.spi.impl.operationservice.impl.OperationServiceAccessor.getAsyncOperationsCount;
import static com.hazelcast.spi.impl.operationservice.impl.OperationServiceAccessor.toStringAsyncOperations;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MapStoreOffloadingBouncingNodeTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "writeBehindEnabled: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Parameterized.Parameter
    public boolean writeBehindEnabled;

    private static final int TEST_RUN_SECONDS = 20;

    @Test(timeout = 5 * 60 * 1000)
    public void stress() throws InterruptedException {
        final String mapName = "map-name";
        final int keySpace = 1_000;

        final Config config = getConfig(mapName);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<String, String> map = node1.getMap(mapName);

        AtomicBoolean stop = new AtomicBoolean(false);
        int availableProcessors = Math.min(6, RuntimeAvailableProcessors.get());
        ExecutorService executorService = Executors.newFixedThreadPool(availableProcessors);

        for (int i = 0; i < availableProcessors - 1; i++) {
            executorService.submit(() -> {
                while (!stop.get()) {
                    OpType[] values = OpType.values();
                    OpType op = values[RandomPicker.getInt(values.length)];
                    op.doOp(map, RandomPicker.getInt(2, keySpace));
                }
            });
        }

        executorService.submit(() -> {
            while (!stop.get()) {
                HazelcastInstance node3 = factory.newHazelcastInstance(config);
                sleepSeconds(2);
                node3.shutdown();
            }
        });

        sleepSeconds(TEST_RUN_SECONDS);

        stop.set(true);
        executorService.shutdown();
        if (!executorService.awaitTermination(60, SECONDS)) {
            executorService.shutdownNow();
        }

        assertTrueEventually(() -> {
            Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
            for (HazelcastInstance node : instances) {
                if (node.getLifecycleService().isRunning()) {
                    int asyncOperationsCount = getAsyncOperationsCount(node);
                    int pendingOffloadedOpCount = getPendingOffloadedOpCount(node.getMap(mapName));

                    assertEquals(format("Found asyncOperationsCount=%d {%s}",
                            asyncOperationsCount, toStringAsyncOperations(node)), 0, asyncOperationsCount);
                    assertEquals(format("Found pendingOffloadedOpCount=%d", pendingOffloadedOpCount),
                            0, pendingOffloadedOpCount);
                }
            }
        });
    }

    protected Config getConfig(String mapName) {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(getInMemoryFormat())
                .getMapStoreConfig()
                .setEnabled(true)
                .setOffload(true)
                .setWriteDelaySeconds(writeBehindEnabled ? 3 : 0)
                .setImplementation(new MapStoreAdapter() {

                    @Override
                    public void store(Object key, Object value) {
                        sleepRandomMillis();
                        super.store(key, value);
                    }

                    @Override
                    public void delete(Object key) {
                        sleepRandomMillis();
                        super.delete(key);
                    }

                    @Override
                    public Object load(Object key) {
                        sleepRandomMillis();
                        return randomStringOrNull();
                    }
                });
        return config;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    private static Object randomStringOrNull() {
        return RandomPicker.getInt(0, 10) < 2
                ? null : randomString();
    }

    private static void sleepRandomMillis() {
        sleepMillis(RandomPicker.getInt(0, 3));
    }

    private enum OpType {
        SET {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.set(toKey(i), "value");
                }
            }
        },

        PUT {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.put(toKey(i), "value");
                }
            }
        },

        PUT_ALL {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                HashMap<String, String> batch = new HashMap<>();
                for (int i = 0; i < keySpace; i++) {
                    batch.put(toKey(i), randomString());
                }

                map.putAll(map);
            }
        },

        REMOVE_ASYNC {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.removeAsync(toKey(i));
                }
            }
        },

        REPLACE_IF_SAME {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.replace(toKey(i), "item-" + i);
                }
            }
        },

        REMOVE_WITH_ENTRY_PROCESSOR {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.executeOnKey(toKey(i), (EntryProcessor) entry -> {
                        entry.setValue(null);
                        return null;
                    });
                }
            }
        },

        REMOVE_WITH_ENTRY_PROCESSOR_BATCH {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                Set<String> keys = new HashSet<>();
                for (int i = 0; i < keySpace; i++) {
                    keys.add(toKey(i));
                }

                map.executeOnKeys(keys, (EntryProcessor) entry -> {
                    entry.setValue(null);
                    return null;
                });
            }
        },

        PUT_WITH_ENTRY_PROCESSOR {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.executeOnKey(toKey(i), (EntryProcessor) entry -> {
                        entry.setValue(randomString());
                        return null;
                    });
                }
            }
        },

        PUT_WITH_ENTRY_PROCESSOR_BATCH {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                Set<String> keys = new HashSet<>();
                for (int i = 0; i < keySpace; i++) {
                    keys.add(toKey(i));
                }

                map.executeOnKeys(keys, (EntryProcessor) entry -> {
                    entry.setValue(randomString());
                    return null;
                });
            }
        },

        GET {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.get(toKey(i));
                }
            }
        },

        GET_ALL {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                Set<String> keys = new HashSet<>();
                for (int i = 0; i < keySpace; i++) {
                    keys.add(toKey(i));
                }

                map.getAll(keys);
            }
        },

        CLEAR {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                map.clear();
            }
        },

        CONTAINS_KEY {
            @Override
            void doOp(IMap<String, String> map, int keySpace) {
                for (int i = 0; i < keySpace; i++) {
                    map.containsKey(toKey(i));
                }
            }
        };

        @Nonnull
        private static String toKey(int i) {
            return "item-" + i;
        }


        OpType() {
        }

        abstract void doOp(IMap<String, String> map, int keySpace);
    }

}
