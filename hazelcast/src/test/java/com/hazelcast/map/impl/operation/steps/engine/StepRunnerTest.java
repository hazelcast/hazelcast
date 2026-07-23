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

package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.map.impl.operation.steps.PutOpSteps;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StepRunnerTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "stepRunnerTest";
    private static final int PARTITION_ID = 1;
    private static final String PARTITION_TASK = "partition-task";
    private static final String SECOND_OPERATION = "second-operation";

    @Test
    public void exceededMaxRunTime_afterLastStep_yieldsBeforeNextOperation() throws Exception {
        BlockingMapStore mapStore = new BlockingMapStore();
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(MapServiceContext.MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS.getName(), "1");
        config.getMapConfig(MAP_NAME)
                .setMapStoreConfig(new MapStoreConfig()
                        .setEnabled(true)
                        .setOffload(true)
                        .setImplementation(mapStore));

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(MAP_NAME).size();

        List<String> executionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch responses = new CountDownLatch(2);

        Data firstKey = Accessors.getSerializationService(node).toData("first-key");
        Data secondKey = Accessors.getSerializationService(node).toData("second-key");
        Data value = Accessors.getSerializationService(node).toData("value");

        SetOperation firstOperation = new SetOperation(MAP_NAME, firstKey, value) {
            @Override
            public void afterRunInternal() {
                super.afterRunInternal();
                getNodeEngine().getOperationService().execute(new PartitionSpecificRunnable() {
                    @Override
                    public int getPartitionId() {
                        return PARTITION_ID;
                    }

                    @Override
                    public void run() {
                        executionOrder.add(PARTITION_TASK);
                    }
                });
            }
        };
        initOperation(node, firstOperation, responses);

        SetOperation secondOperation = new SetOperation(MAP_NAME, secondKey, value) {
            @Override
            public Step getStartingStep() {
                return new Step<State>() {
                    @Override
                    public void runStep(State state) {
                        executionOrder.add(SECOND_OPERATION);
                        PutOpSteps.READ.runStep(state);
                    }

                    @Override
                    public Step nextStep(State state) {
                        return PutOpSteps.READ.nextStep(state);
                    }
                };
            }
        };
        initOperation(node, secondOperation, responses);

        Accessors.getOperationService(node).execute(firstOperation);
        assertTrue("first operation did not reach MapStore.store()",
                mapStore.storeEntered.await(10, TimeUnit.SECONDS));

        Accessors.getOperationService(node).execute(secondOperation);
        assertTrueEventually(() -> assertEquals(2,
                firstOperation.getRecordStore().getOffloadedOperations().size()));

        mapStore.proceed.countDown();

        assertOpenEventually(responses);
        assertTrueEventually(() -> assertEquals(2, executionOrder.size()));
        assertEquals(List.of(PARTITION_TASK, SECOND_OPERATION), executionOrder);
    }

    private static void initOperation(HazelcastInstance node, Operation operation, CountDownLatch responses) {
        operation.setPartitionId(PARTITION_ID)
                .setNodeEngine(Accessors.getNodeEngineImpl(node))
                .setOperationResponseHandler((op, response) -> responses.countDown());
    }

    private static final class BlockingMapStore extends MapStoreAdapter<Object, Object> {

        private final CountDownLatch storeEntered = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);

        @Override
        public void store(Object key, Object value) {
            storeEntered.countDown();
            try {
                proceed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public Object load(Object key) {
            return null;
        }
    }
}
