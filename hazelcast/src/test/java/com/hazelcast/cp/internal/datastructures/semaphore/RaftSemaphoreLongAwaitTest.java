/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSemaphoreLongAwaitTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private String objectName = "semaphore";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;
    private CPGroupId groupId;
    private final long callTimeoutSeconds = 15;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        groupId = ((RaftSessionAwareSemaphoreProxy) instances[0].getCPSubsystem().getSemaphore(proxyName)).getGroupId();
    }

    @Test(timeout = 300000)
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromLeaderInstanceWaits() throws ExecutionException, InterruptedException {
        testLongAwait(getLeaderInstance(instances, groupId));
    }

    @Test(timeout = 300000)
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromNonLeaderInstanceWaits() throws ExecutionException, InterruptedException {
        testLongAwait(getRandomFollowerInstance(instances, groupId));
    }

    private void testLongAwait(final HazelcastInstance instance) throws ExecutionException, InterruptedException {
        final ISemaphore semaphore = instance.getCPSubsystem().getSemaphore(proxyName);

        Future<Object> f1 = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                if (!semaphore.tryAcquire(1, 5, TimeUnit.MINUTES)) {
                    throw new IllegalStateException();
                }

                semaphore.release();

                return null;
            }
        });

        Future<Object> f2 = spawn(new Callable<Object>() {
            @Override
            public Object call() throws InterruptedException {
                semaphore.acquire();
                semaphore.release();

                return null;
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(instance).getService(RaftSemaphoreService.SERVICE_NAME);
                assertEquals(2, service.getLiveOperations(groupId).size());
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(instance).getService(RaftSemaphoreService.SERVICE_NAME);
                assertEquals(2, service.getLiveOperations(groupId).size());
            }
        }, callTimeoutSeconds + 5);

        semaphore.increasePermits(1);

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);

        f1.get();
        f2.get();
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        String callTimeoutStr = String.valueOf(SECONDS.toMillis(callTimeoutSeconds));
        return super.createConfig(cpNodeCount, groupSize)
                    .setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), callTimeoutStr);
    }

}
