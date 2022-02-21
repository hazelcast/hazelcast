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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreLongAwaitTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private String objectName = "semaphore";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;
    private CPGroupId groupId;
    private final long callTimeoutSeconds = 15;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        groupId = ((SessionAwareSemaphoreProxy) instances[0].getCPSubsystem().getSemaphore(proxyName)).getGroupId();
    }

    @Test(timeout = 300000)
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromLeaderInstanceWaits() throws ExecutionException, InterruptedException {
        testLongAwait(getLeaderInstance(instances, groupId));
    }

    @Test(timeout = 300000)
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromNonLeaderInstanceWaits() throws ExecutionException, InterruptedException {
        testLongAwait(getRandomFollowerInstance(instances, groupId));
    }

    private void testLongAwait(HazelcastInstance instance) throws ExecutionException, InterruptedException {
        ISemaphore semaphore = instance.getCPSubsystem().getSemaphore(proxyName);

        Future<Object> f1 = spawn(() -> {
            if (!semaphore.tryAcquire(1, 5, TimeUnit.MINUTES)) {
                throw new IllegalStateException();
            }

            semaphore.release();

            return null;
        });

        Future<Object> f2 = spawn(() -> {
            semaphore.acquire();
            semaphore.release();

            return null;
        });

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(instance).getService(SemaphoreService.SERVICE_NAME);
            assertEquals(2, service.getLiveOperations(groupId).size());
        });

        assertTrueAllTheTime(() -> {
            SemaphoreService service = getNodeEngineImpl(instance).getService(SemaphoreService.SERVICE_NAME);
            assertEquals(2, service.getLiveOperations(groupId).size());
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
                    .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), callTimeoutStr);
    }

}
