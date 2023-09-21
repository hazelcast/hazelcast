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
package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.map.impl.operation.steps.IMapOpStep;
import com.hazelcast.map.impl.operation.steps.PutOpSteps;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StepSupplierTest extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void step_supplier_finishes() throws Exception {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        Data data = Accessors.getSerializationService(node).toData("data");
        MapOperation operation = new SetOperation("map", data, data);
        operation.setNodeEngine(Accessors.getNodeEngineImpl(node));
        operation.setPartitionId(1);
        operation.beforeRun();

        try {
            StepSupplier stepSupplier = new StepSupplier(operation, false);
            Runnable step;
            while ((step = stepSupplier.get()) != null) {
                step.run();
            }
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void step_supplier_get_returns_same_step() throws Exception {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        Data data = Accessors.getSerializationService(node).toData("data");
        MapOperation operation = new SetOperation("map", data, data);
        operation.setNodeEngine(Accessors.getNodeEngineImpl(node));
        operation.setPartitionId(1);
        operation.beforeRun();

        StepSupplier stepSupplier = new StepSupplier(operation);
        Runnable get1 = stepSupplier.get();
        Runnable get2 = stepSupplier.get();
        assertEquals(get1, get2);
    }

    @Test
    public void step_supplier_ends_with_call_timeout_response_when_operation_timed_out() {
        // create node with force offload
        Config config = getConfig();
        config.setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(), "true");
        HazelcastInstance node = createHazelcastInstance(config);

        // create state to use for test verification
        AtomicReference<Object> expectedResponse = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        // create and call slow operation
        Data data = Accessors.getSerializationService(node).toData("data");
        int partitionId = Accessors.getPartitionService(node).getPartitionId(data);

        MapOperation operation = new SetOperation("test-map", data, data) {
            @Override
            protected void innerBeforeRun() throws Exception {
                super.innerBeforeRun();
                sleepAtLeastSeconds(2);
            }
        };
        operation.setNodeEngine(Accessors.getNodeEngineImpl(node));
        operation.setPartitionId(partitionId);
        operation.setServiceName(MapService.SERVICE_NAME);
        operation.setOperationResponseHandler((op, response) -> {
            expectedResponse.set(response);
            latch.countDown();
        });

        // set op times out after 1 second
        OperationAccessor.setCallTimeout(operation, 1000);
        OperationAccessor.setInvocationTime(operation, Clock.currentTimeMillis());
        Accessors.getOperationService(node).execute(operation);

        // wait operation end
        assertOpenEventually(latch);
        assertInstanceOf(CallTimeoutResponse.class, expectedResponse.get());
    }

    @Test
    public void firstStepIsNotExecuted_whenOperationTimesOut() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(), "true");
        HazelcastInstance node = createHazelcastInstance(config);

        // latch for to be sure operation is executed
        CountDownLatch latch = new CountDownLatch(1);

        // create and call slow operation
        Data data = Accessors.getSerializationService(node).toData("data");
        int partitionId = Accessors.getPartitionService(node).getPartitionId(data);

        MapOperation operation = new SetOperation("test-map", data, data) {
            @Override
            protected void innerBeforeRun() throws Exception {
                super.innerBeforeRun();
                sleepAtLeastSeconds(2);
            }

            @Override
            public Step getStartingStep() {
                return DummyPutOpSteps.DUMMY_READ;
            }
        };

        operation.setNodeEngine(Accessors.getNodeEngineImpl(node));
        operation.setPartitionId(partitionId);
        operation.setServiceName(MapService.SERVICE_NAME);
        operation.setOperationResponseHandler((op, response) -> latch.countDown());

        // set op times out after 1 second
        OperationAccessor.setCallTimeout(operation, 1000);
        OperationAccessor.setInvocationTime(operation, Clock.currentTimeMillis());
        Accessors.getOperationService(node).execute(operation);

        // wait operation end
        assertOpenEventually(latch);
        assertFalse(DummyPutOpSteps.executed);
    }

    private enum DummyPutOpSteps implements IMapOpStep {
        DUMMY_READ {
            @Override
            public void runStep(State state) {
                executed = true;
                PutOpSteps.READ.runStep(state);
            }

            @Override
            public Step nextStep(State state) {
                return PutOpSteps.READ.nextStep(state);
            }
        };

        private static volatile boolean executed;
    }

    @Test
    public void step_supplier_handles_rejected_execution_exception_then_operations_finish() {
        Config config = getConfig();
        // configure offloadable executor to throw
        // RejectedExecutionException quickly
        ExecutorConfig executorConfig
                = config.getExecutorConfig(ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR);
        executorConfig.setPoolSize(1).setQueueCapacity(1);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setEnabled(true)
                .setImplementation(new MapStoreAdapter<String, String>());

        String mapName = "default";
        config.getMapConfig(mapName)
                .setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, String> map = instance.getMap(mapName);

        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            futures.add(map.setAsync("key-" + i, String.valueOf(i)).toCompletableFuture());
        }

        FutureUtil.waitUntilAllResponded(futures);
    }

    @Test
    public void head_steps_are_ran_in_provided_order() throws Exception {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        Data data = Accessors.getSerializationService(node).toData("data");
        MapOperation operation = new SetOperation("map", data, data);
        operation.setNodeEngine(Accessors.getNodeEngineImpl(node));
        operation.setPartitionId(1);
        operation.beforeRun();

        List<Integer> actualExecutionOrder = new ArrayList<>();
        List<Step> expectedSteps = new ArrayList<>();

        int headSteps = 10;
        for (int i = 0; i < headSteps; i++) {
            int finalI = i;
            expectedSteps.add(state -> actualExecutionOrder.add(finalI));
        }

        StepSupplier stepSupplier = new StepSupplier(operation, false);
        for (int i = expectedSteps.size() - 1; i >= 0; i--) {
              stepSupplier.accept(expectedSteps.get(i));
        }

        List<Step> actualSteps = new ArrayList<>();
        for (int i = 0; i < headSteps; i++) {
            Step currentStep = stepSupplier.getCurrentStep();
            actualSteps.add(((AppendAsNewHeadStep) currentStep).getAppendedStep());

            // execute current step to move next one.
            Runnable nextStepsRunnable = stepSupplier.get();
            if (nextStepsRunnable != null) {
                nextStepsRunnable.run();
            }
        }

        assertTrue(expectedSteps.equals(actualSteps));
        int[] expectedExecutions = IntStream.range(0, headSteps).toArray();
        int[] actualExecutions = actualExecutionOrder.stream().mapToInt(i -> i).toArray();
        assertArrayEquals(expectedExecutions, actualExecutions);
    }
}
