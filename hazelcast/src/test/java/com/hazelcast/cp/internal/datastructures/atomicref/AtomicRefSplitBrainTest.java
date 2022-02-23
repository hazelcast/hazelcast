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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.RaftSplitBrainTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicRefSplitBrainTest extends RaftSplitBrainTestSupport {

    private final String name = "atomic";
    private final AtomicBoolean done = new AtomicBoolean();
    private final AtomicLong increments = new AtomicLong();
    private final AtomicLong indeterminate = new AtomicLong();
    private Future[] futures;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitUntilCPDiscoveryCompleted(instances);

        IAtomicReference<Long> atomic = instances[0].getCPSubsystem().getAtomicReference(name);
        atomic.set(0L);

        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(new Adder(instances[i]));
        }
        sleepSeconds(3);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        sleepSeconds(5);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        sleepSeconds(3);
        done.set(true);
        for (Future future : futures) {
            assertCompletesEventually(future);
            future.get();
        }
        IAtomicReference<Long> atomic = instances[0].getCPSubsystem().getAtomicReference(name);
        assertThat(atomic.get(), greaterThanOrEqualTo(increments.get()));
        assertThat(atomic.get(), lessThanOrEqualTo(increments.get() + indeterminate.get()));
    }

    private class Adder implements Runnable {
        private final HazelcastInstance instance;

        Adder(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            IAtomicReference<Long> atomic = instance.getCPSubsystem().getAtomicReference(name);
            while (!done.get()) {
                Long value = atomic.get();
                try {
                    if (atomic.compareAndSet(value, value + 1)) {
                        increments.incrementAndGet();
                    }
                } catch (IndeterminateOperationStateException | OperationTimeoutException e) {
                    indeterminate.incrementAndGet();
                }
            }
        }
    }
}
