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

package com.hazelcast.executor;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorServiceCancelTest extends ExecutorServiceTestSupport {

    private HazelcastInstance localHz;
    private HazelcastInstance remoteHz;
    private String taskStartedLatchName;
    private ICountDownLatch taskStartedLatch;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        localHz = factory.newHazelcastInstance(smallInstanceConfig());
        remoteHz = factory.newHazelcastInstance(smallInstanceConfig());
        taskStartedLatchName = randomName();
        taskStartedLatch = localHz.getCPSubsystem().getCountDownLatch(taskStartedLatchName);
        taskStartedLatch.trySetCount(1);
    }

    @Test
    public void testCancel_submitRandom() {
        IExecutorService executorService = localHz.getExecutorService(randomString());
        Future<Boolean> future = executorService.submit(new SleepingTask(Integer.MAX_VALUE, taskStartedLatchName));
        awaitTaskStart();

        boolean result = future.cancel(true);

        assertTrue(result);
    }

    public void awaitTaskStart() {
        assertTrueEventually(() -> assertEquals(0, taskStartedLatch.getCount()));
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitRandom() throws Exception {
        IExecutorService executorService = localHz.getExecutorService(randomString());
        Future<Boolean> future = executorService.submit(new SleepingTask(Integer.MAX_VALUE, taskStartedLatchName));
        awaitTaskStart();

        future.cancel(true);

        future.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testCancel_submitToLocalMember() {
        testCancel_submitToMember(localHz, localHz.getCluster().getLocalMember());
    }

    @Test
    public void testCancel_submitToRemoteMember() {
        testCancel_submitToMember(localHz, remoteHz.getCluster().getLocalMember());
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitToLocalMember() throws Exception {
        testGetValueAfterCancel_submitToMember(localHz, localHz.getCluster().getLocalMember());
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitToRemoteMember() throws Exception {
        testGetValueAfterCancel_submitToMember(localHz, remoteHz.getCluster().getLocalMember());
    }

    private void testCancel_submitToMember(HazelcastInstance instance, Member member) {
        IExecutorService executorService = instance.getExecutorService(randomString());
        Future<Boolean> future
                = executorService.submitToMember(new SleepingTask(Integer.MAX_VALUE, taskStartedLatchName), member);
        awaitTaskStart();

        assertTrue(future.cancel(true));
    }

    private void testGetValueAfterCancel_submitToMember(HazelcastInstance instance, Member member) throws Exception {
        IExecutorService executorService = instance.getExecutorService(randomString());
        Future<Boolean> future
                = executorService.submitToMember(new SleepingTask(Integer.MAX_VALUE, taskStartedLatchName), member);
        awaitTaskStart();

        future.cancel(true);
        future.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testCancel_submitToKeyOwner() {
        IExecutorService executorService = localHz.getExecutorService(randomString());
        Future<Boolean> future
                = executorService.submitToKeyOwner(new SleepingTask(Integer.MAX_VALUE, taskStartedLatchName), randomString());
        awaitTaskStart();


        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
    }

    @Test(expected = CancellationException.class)
    public void testGetValueAfterCancel_submitToKeyOwner() throws Exception {
        IExecutorService executorService = localHz.getExecutorService(randomString());
        Future<Boolean> future
                = executorService.submitToKeyOwner(new SleepingTask(Integer.MAX_VALUE, taskStartedLatchName), randomString());
        awaitTaskStart();

        future.cancel(true);
        future.get(10, TimeUnit.SECONDS);
    }

    static class SleepingTask implements Callable<Boolean>, Serializable, PartitionAware<String>, HazelcastInstanceAware {
        private final String taskStartedLatchName;
        private long sleepSeconds;
        private HazelcastInstance hz;

        SleepingTask(long sleepSeconds, String taskStartedLatchName) {
            this.sleepSeconds = sleepSeconds;
            this.taskStartedLatchName = taskStartedLatchName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public Boolean call() throws InterruptedException {
            hz.getCPSubsystem().getCountDownLatch(taskStartedLatchName).countDown();

            sleepSeconds((int) sleepSeconds);
            return true;
        }

        @Override
        public String getPartitionKey() {
            return "key";
        }
    }
}
