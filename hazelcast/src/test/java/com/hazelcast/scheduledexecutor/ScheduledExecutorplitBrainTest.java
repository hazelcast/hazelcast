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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorplitBrainTest
        extends SplitBrainTestSupport {

    private final String name = randomString();
    private final int initialCount = 100;
    private final int finalCount = initialCount + 50;

    private Map<String, IScheduledFuture<Double>> allScheduledFutures = new HashMap<String, IScheduledFuture<Double>>();

    static {
        System.setProperty("hazelcast.test.use.network", "false");
        System.out.println(TestEnvironment.isMockNetwork());
    }
    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(name);

        for (int i = 0; i < initialCount; i++) {
            schedule(executorService, i);
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {

        IScheduledExecutorService executorService1 = firstBrain[0].getScheduledExecutorService(name);
        for (int i = initialCount; i < finalCount; i++) {
            schedule(executorService1, i);
        }

        IScheduledExecutorService executorService2 = secondBrain[0].getScheduledExecutorService(name);
        for (int i = initialCount; i < finalCount + 10; i++) {
            schedule(executorService2, i);
        }
    }

    private void schedule(IScheduledExecutorService scheduledExecutorService, int value) {
        // Once task runs, all calls to `.get()` should return 30
        String name = String.valueOf(value);
        allScheduledFutures.put(name, scheduledExecutorService.schedule(
                named(name, new ScheduledExecutorServiceTestSupport.PlainCallableTask(5)), 0, SECONDS));
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances)
            throws ExecutionException, InterruptedException {
        IScheduledExecutorService executorService = instances[0].getScheduledExecutorService(name);
        assertContents(executorService.<Double>getAllScheduledFutures());
        assertHandlersAreStillCorrect();
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
