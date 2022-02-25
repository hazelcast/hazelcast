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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftSplitBrainTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static java.util.concurrent.locks.LockSupport.parkNanos;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(SlowTest.class)
public class SemaphoreSplitBrainTest extends RaftSplitBrainTestSupport {

    private final String name = "semaphore";
    private final int initialPermits = 2;
    private final AtomicBoolean done = new AtomicBoolean();
    private Future[] futures;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitUntilCPDiscoveryCompleted(instances);

        ISemaphore sema = instances[0].getCPSubsystem().getSemaphore(name);
        sema.init(initialPermits);

        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(new Locker(instances[i % instances.length]));
        }
        sleepSeconds(3);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        sleepSeconds(withSessionTimeout ? 10 : 5);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        sleepSeconds(3);
        done.set(true);
        for (Future future : futures) {
            assertCompletesEventually(future);
            future.get();
        }
    }

    private class Locker implements Callable {
        private final HazelcastInstance instance;

        Locker(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public Object call() throws Exception {
            ISemaphore sema = instance.getCPSubsystem().getSemaphore(name);
            while (!done.get()) {
                try {
                    sema.acquire();
                    parkNanos(1);
                    sema.release();
                } catch (IllegalStateException e) {
                    // means session timeout or no session
                    e.printStackTrace();
                } catch (OperationTimeoutException ignored) {
                }
            }
            return null;
        }
    }
}
