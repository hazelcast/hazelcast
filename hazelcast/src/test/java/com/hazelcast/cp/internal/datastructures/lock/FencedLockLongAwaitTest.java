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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.lock.FencedLock;
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

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockLongAwaitTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private String objectName = "lock";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;
    private CPGroupId groupId;
    private final long callTimeoutSeconds = 15;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        groupId = instances[0].getCPSubsystem().getLock(proxyName).getGroupId();
    }

    @Test(timeout = 300000)
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromLeaderInstanceWaits() throws ExecutionException, InterruptedException {
        testLongAwait(getLeaderInstance(instances, groupId));
    }

    @Test(timeout = 300000)
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromNonLeaderInstanceWaits() throws ExecutionException, InterruptedException {
        testLongAwait(getRandomFollowerInstance(instances, groupId));
    }

    @Test(timeout = 300000)
    public void when_longWaitOperationIsNotCommitted_then_itFailsWithOperationTimeoutException() {
        HazelcastInstance apInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        FencedLock lock = apInstance.getCPSubsystem().getLock(proxyName);
        spawn(lock::lock);

        assertTrueEventually(() -> assertTrue(lock.isLocked()));

        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        for (int i = 0; i < groupSize; i++) {
            HazelcastInstance instance = instances[i];
            if (instance != leader) {
                instance.getLifecycleService().terminate();
            }
        }

        try {
            lock.lock();
            fail();
        } catch (OperationTimeoutException ignored) {
        }
    }

    private void testLongAwait(HazelcastInstance instance) throws ExecutionException, InterruptedException {
        FencedLock lock = instance.getCPSubsystem().getLock(proxyName);
        lock.lock();

        Future<Object> f1 = spawn(() -> {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException();
            }

            lock.unlock();

            return null;
        });

        Future<Object> f2 = spawn(() -> {
            lock.lock();
            lock.unlock();

            return null;
        });

        assertTrueEventually(() -> {
            LockService service = getNodeEngineImpl(instance).getService(LockService.SERVICE_NAME);
            assertEquals(2, service.getLiveOperations(groupId).size());
        }, 30);

        assertTrueAllTheTime(() -> {
            LockService service = getNodeEngineImpl(instance).getService(LockService.SERVICE_NAME);
            assertEquals(2, service.getLiveOperations(groupId).size());
        }, callTimeoutSeconds + 5);

        lock.unlock();

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);

        f1.get();
        f2.get();
    }

    @Test(timeout = 300000)
    public void when_tryLockTimeoutPassesDuringLostMajority_then_operationTimeoutIsReceived() throws Exception {
        HazelcastInstance apInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        FencedLock lock = apInstance.getCPSubsystem().getLock(proxyName);

        lock.lock();

        Future<Object> future = spawn(() -> lock.tryLock(callTimeoutSeconds + 5, SECONDS));

        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        assertTrueEventually(() -> {
            LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
            assertEquals(1, service.getLiveOperations(groupId).size());
        });

        for (HazelcastInstance instance : instances) {
            if (instance != leader) {
                instance.getLifecycleService().terminate();
            }
        }

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof OperationTimeoutException);
        }
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        String callTimeoutStr = String.valueOf(SECONDS.toMillis(callTimeoutSeconds));
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setMaxMissedLeaderHeartbeatCount(10);
        return config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), callTimeoutStr);
    }

}
