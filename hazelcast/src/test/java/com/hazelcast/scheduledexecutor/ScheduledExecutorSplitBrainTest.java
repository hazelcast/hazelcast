/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore(value = "https://github.com/hazelcast/hazelcast/issues/12255")
public class ScheduledExecutorSplitBrainTest
        extends SplitBrainTestSupport {

    private final String name = randomString();

    private final int INITIAL_COUNT = 100;
    private final int FINAL_COUNT = INITIAL_COUNT + 50;

    private MergeLifecycleListener mergeLifecycleListener;

    @Parameterized.Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                PutIfAbsentMergePolicy.class
        });
    }

    @Parameterized.Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private Map<String, IScheduledFuture<Double>> allScheduledFutures = new HashMap<String, IScheduledFuture<Double>>();

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getScheduledExecutorConfig(name)
              .setDurability(1)
              .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(name);

        for (int i = 0; i < INITIAL_COUNT; i++) {
            schedule(executorService, i);
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterSplitDiscardPolicy(firstBrain, secondBrain);
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterSplitPutAbsentPolicy(firstBrain, secondBrain);
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances)
            throws ExecutionException, InterruptedException {
        // wait until merge completes
        mergeLifecycleListener.await();

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterMergeDiscardMergePolicy(instances);
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterMergePutAbsentMergePolicy(instances);
        } else {
            fail();
        }
    }

    private void onAfterSplitDiscardPolicy(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        IScheduledExecutorService scheduledExecutorService = secondBrain[0].getScheduledExecutorService(name);
        schedule(scheduledExecutorService, Integer.MAX_VALUE);
    }

    private void onAfterMergeDiscardMergePolicy(HazelcastInstance[] instances)
            throws ExecutionException, InterruptedException {
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(name);

        // Remove the task that was scheduled after the split
        IScheduledFuture afterSplitScheduledTask = allScheduledFutures.remove(String.valueOf(Integer.MAX_VALUE));

        // Assert everything else (ie. tasks created before split) is in order
        assertContents(executorService.<Double>getAllScheduledFutures());
        assertHandlersAreStillCorrect();

        // Attempting to access the task that was scheduled after the split should fail
        try {
            afterSplitScheduledTask.isDone();
            fail();
        } catch (StaleTaskException ste) {
            ignore(ste);
        }
    }

    private void onAfterSplitPutAbsentPolicy(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        IScheduledExecutorService executorService1 = firstBrain[0].getScheduledExecutorService(name);
        for (int i = INITIAL_COUNT; i < FINAL_COUNT; i++) {
            schedule(executorService1, i);
        }

        IScheduledExecutorService executorService2 = secondBrain[0].getScheduledExecutorService(name);
        for (int i = INITIAL_COUNT; i < FINAL_COUNT + 10; i++) {
            schedule(executorService2, i);
        }
    }

    private void onAfterMergePutAbsentMergePolicy(HazelcastInstance[] instances)
            throws ExecutionException, InterruptedException {
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(name);
        assertContents(executorService.<Double>getAllScheduledFutures());
        assertHandlersAreStillCorrect();
    }

    private void schedule(IScheduledExecutorService scheduledExecutorService, int value) {
        // Once task runs, all calls to `.get()` should return 30
        String name = String.valueOf(value);
        allScheduledFutures.put(name, scheduledExecutorService.schedule(
                named(name, new ScheduledExecutorServiceTestSupport.PlainCallableTask(5)), 0, SECONDS));
    }

    private void assertContents(Map<Member, List<IScheduledFuture<Double>>> futuresPerMember)
            throws ExecutionException, InterruptedException {

        int total = 0;
        for (List<IScheduledFuture<Double>> memberFutures : futuresPerMember.values()) {
            total += memberFutures.size();
        }

        assertEquals(allScheduledFutures.size(), total);

        Set<String> seenSoFar = new HashSet<String>();
        for (List<IScheduledFuture<Double>> memberFutures : futuresPerMember.values()) {
            for (IScheduledFuture<Double> future : memberFutures) {
                String taskName = future.getHandler().getTaskName();
                double value = future.get();

                assertThat(Integer.parseInt(future.getHandler().getTaskName()),
                        allOf(greaterThanOrEqualTo(0), lessThan(allScheduledFutures.size())));
                assertEquals(value, 30, 0);
                assertFalse(seenSoFar.contains(taskName));
                seenSoFar.add(taskName);
            }
        }
    }

    private void assertHandlersAreStillCorrect()
            throws ExecutionException, InterruptedException {
        List<IScheduledFuture<Double>> allFutures = new ArrayList<IScheduledFuture<Double>>(allScheduledFutures.values());
        Collections.sort(allFutures, new Comparator<IScheduledFuture<Double>>() {
            @Override
            public int compare(IScheduledFuture<Double> o1, IScheduledFuture<Double> o2) {
                int a = Integer.parseInt(o1.getHandler().getTaskName());
                int b = Integer.parseInt(o2.getHandler().getTaskName());
                return new Integer(a).compareTo(b);
            }
        });

        int counter = 0;
        for (IScheduledFuture<Double> future : allFutures) {
            // Make sure handler is still valid and no exceptions are thrown
            assertEquals(counter++, Integer.parseInt(future.getHandler().getTaskName()));
            assertEquals(30, future.get(), 0);
        }
    }
}
