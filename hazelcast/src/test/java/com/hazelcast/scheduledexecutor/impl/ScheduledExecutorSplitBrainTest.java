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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceTestSupport.PlainCallableTask;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledExecutorSplitBrainTest extends SplitBrainTestSupport {

    private static final int INITIAL_COUNT = 300;
    private static final int AFTER_SPLIT_COMMON_COUNT = INITIAL_COUNT + 50;
    private static final int FINAL_COUNT = AFTER_SPLIT_COMMON_COUNT + 50;

    // with these values the expected result will be 42.0, the unexpected result 100.0
    private static final int EXPECTED_VALUE = 17;
    private static final int UNEXPECTED_VALUE = 75;
    private static final double EXPECTED_RESULT = PlainCallableTask.calculateResult(EXPECTED_VALUE);

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                PassThroughMergePolicy.class,
                PutIfAbsentMergePolicy.class,
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    // the ConcurrentMap just for the convenience of the putIfAbsent(), no real concurrency needs here
    private final ConcurrentMap<String, IScheduledFuture<Double>> expectedScheduledFutures
            = new ConcurrentHashMap<String, IScheduledFuture<Double>>();
    private final ConcurrentMap<String, IScheduledFuture<Double>> unexpectedScheduledFutures
            = new ConcurrentHashMap<String, IScheduledFuture<Double>>();

    private String scheduledExecutorName = randomMapName("scheduledExecutor-");
    private IScheduledExecutorService scheduledExecutorService1;
    private IScheduledExecutorService scheduledExecutorService2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getScheduledExecutorConfig(scheduledExecutorName)
                .setDurability(1)
                .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION)
                .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(scheduledExecutorName);

        for (int i = 0; i < INITIAL_COUNT; i++) {
            schedule(executorService, i, EXPECTED_VALUE);
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        /*
         * Wait for a few seconds, to allow the event system to finish delivering partition-lost events.
         * This minimizes the chances of a race condition between handling the event and scheduling a new task.
         * The IScheduledExecutor allows for tasks to be aware of lost partitions (marking the tasks as stale), hence, if
         * we schedule a task before handling the event, then the task becomes stale (due to the event).
         * Similar too: https://github.com/hazelcast/hazelcast/issues/12424
         */
        sleepSeconds(5);

        scheduledExecutorService1 = firstBrain[0].getScheduledExecutorService(scheduledExecutorName);
        scheduledExecutorService2 = secondBrain[0].getScheduledExecutorService(scheduledExecutorName);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterSplitDiscardPolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            onAfterSplitPassThroughPolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterSplitPutIfAbsentPolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        // wait until merge completes
        mergeLifecycleListener.await();

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            onAfterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterMergePutIfAbsentMergePolicy();
        } else {
            fail();
        }
    }

    private void onAfterSplitDiscardPolicy() {
        schedule(scheduledExecutorService2, MAX_VALUE, UNEXPECTED_VALUE);
    }

    private void onAfterMergeDiscardMergePolicy() throws Exception {
        // assert everything else (ie. tasks created before split) is in order
        assertContents(scheduledExecutorService1.<Double>getAllScheduledFutures());
        assertContents(scheduledExecutorService2.<Double>getAllScheduledFutures());
        assertHandlersAreStillCorrect();
        assertDiscardedFutures();
    }

    private void onAfterSplitPassThroughPolicy() {
        // we should not see the tasks with UNEXPECTED_VALUE in the final tasks,
        // since they will be overridden by the merging tasks with the same name
        for (int i = INITIAL_COUNT; i < AFTER_SPLIT_COMMON_COUNT; i++) {
            schedule(scheduledExecutorService1, i, UNEXPECTED_VALUE);
            schedule(scheduledExecutorService2, i, EXPECTED_VALUE);
        }

        // we should not lose these additional tasks, since they have a unique name
        for (int i = AFTER_SPLIT_COMMON_COUNT; i < FINAL_COUNT; i++) {
            schedule(scheduledExecutorService2, i, EXPECTED_VALUE);
        }
    }

    private void onAfterMergePassThroughMergePolicy() throws Exception {
        assertContents(scheduledExecutorService1.<Double>getAllScheduledFutures());
        assertContents(scheduledExecutorService2.<Double>getAllScheduledFutures());
        assertHandlersAreStillCorrect();
        assertUnexpectedFuturesHaveMergedValue();
    }

    private void onAfterSplitPutIfAbsentPolicy() {
        // we should not see the tasks with UNEXPECTED_VALUE in the final tasks,
        // since they have the same name as existing tasks
        for (int i = INITIAL_COUNT; i < AFTER_SPLIT_COMMON_COUNT; i++) {
            schedule(scheduledExecutorService1, i, EXPECTED_VALUE);
            schedule(scheduledExecutorService2, i, UNEXPECTED_VALUE);
        }

        // we should not lose these additional tasks, since they have a unique name
        for (int i = AFTER_SPLIT_COMMON_COUNT; i < FINAL_COUNT; i++) {
            schedule(scheduledExecutorService2, i, EXPECTED_VALUE);
        }
    }

    private void onAfterMergePutIfAbsentMergePolicy() throws Exception {
        assertContents(scheduledExecutorService1.<Double>getAllScheduledFutures());
        assertContents(scheduledExecutorService2.<Double>getAllScheduledFutures());
        assertHandlersAreStillCorrect();
        assertUnexpectedFuturesHaveMergedValue();
    }

    private void schedule(IScheduledExecutorService scheduledExecutorService, int name, int taskValue) {
        // once a task runs, all calls to future.get() should return PlainCallableTask.calculateResult(value)
        String stringName = valueOf(name);
        Callable<Double> task = named(stringName, new PlainCallableTask(taskValue));
        IScheduledFuture<Double> future = scheduledExecutorService.schedule(task, 0, SECONDS);
        if (taskValue == EXPECTED_VALUE) {
            expectedScheduledFutures.putIfAbsent(stringName, future);
        } else {
            unexpectedScheduledFutures.putIfAbsent(stringName, future);
        }
    }

    private void assertContents(Map<Member, List<IScheduledFuture<Double>>> futuresPerMember) throws Exception {
        int total = 0;
        for (List<IScheduledFuture<Double>> memberFutures : futuresPerMember.values()) {
            total += memberFutures.size();
        }

        assertEquals(expectedScheduledFutures.size(), total);

        Set<String> seenSoFar = new HashSet<String>();
        for (List<IScheduledFuture<Double>> memberFutures : futuresPerMember.values()) {
            for (IScheduledFuture<Double> future : memberFutures) {
                String taskName = future.getHandler().getTaskName();
                double value = future.get();

                assertThat(parseInt(future.getHandler().getTaskName()),
                        allOf(greaterThanOrEqualTo(0), lessThan(expectedScheduledFutures.size())));
                assertEquals(EXPECTED_RESULT, value, 0);
                assertFalse(seenSoFar.contains(taskName));
                seenSoFar.add(taskName);
            }
        }
    }

    private void assertHandlersAreStillCorrect() throws Exception {
        List<IScheduledFuture<Double>> allFutures = new ArrayList<IScheduledFuture<Double>>(expectedScheduledFutures.values());
        sort(allFutures, new Comparator<IScheduledFuture<Double>>() {
            @Override
            public int compare(IScheduledFuture<Double> o1, IScheduledFuture<Double> o2) {
                int a = parseInt(o1.getHandler().getTaskName());
                int b = parseInt(o2.getHandler().getTaskName());
                return new Integer(a).compareTo(b);
            }
        });

        int counter = 0;
        for (IScheduledFuture<Double> future : allFutures) {
            // make sure the handler is still valid and no exceptions are thrown
            assertEquals(counter++, parseInt(future.getHandler().getTaskName()));
            assertEquals(EXPECTED_RESULT, future.get(), 0);
        }
    }

    private void assertDiscardedFutures() throws Exception {
        // attempting to access discarded task should fail
        for (Map.Entry<String, IScheduledFuture<Double>> entry : unexpectedScheduledFutures.entrySet()) {
            String taskName = entry.getKey();
            IScheduledFuture<Double> future = entry.getValue();
            try {
                future.isDone();
                fail("The future for task " + taskName + " is still accessible! Result: " + future.get());
            } catch (StaleTaskException e) {
                ignore(e);
            } catch (IllegalStateException e) {
                assertContains(e.getMessage(), "was lost along with all backups.");
            }
        }
    }

    private void assertUnexpectedFuturesHaveMergedValue() throws Exception {
        for (Map.Entry<String, IScheduledFuture<Double>> entry : unexpectedScheduledFutures.entrySet()) {
            String taskName = entry.getKey();
            IScheduledFuture<Double> future = entry.getValue();
            assertTrue("Expected the future for task " + taskName + " to be done", future.isDone());
            assertFalse("Expected the future for task " + taskName + " not to be cancelled", future.isCancelled());
            assertEquals("Expected the future for task " + taskName + " to have the EXPECTED_RESULT " + EXPECTED_RESULT,
                    EXPECTED_RESULT, future.get(), 0);
        }
    }
}
