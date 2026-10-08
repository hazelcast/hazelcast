/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.RaftSplitBrainTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CPMapSplitBrainTest extends RaftSplitBrainTestSupport {

    private final String name = randomMapName("cp-");
    private final String key = "k";

    private final AtomicBoolean done = new AtomicBoolean();
    private final AtomicLong increments = new AtomicLong();
    private final AtomicLong indeterminate = new AtomicLong();
    private Future[] futures;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitUntilCPDiscoveryCompleted(instances);

        CPMap<String, Long> cpMap = instances[0].getCPSubsystem().getMap(name);
        cpMap.set(key, 0L);

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
            try {
                future.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnsupportedOperationException) {
                    // RU_COMPAT_5_3
                    // After the split-brain healed, the member's cluster version is not set immediately,
                    // and the version check in CPMapProxy#synchronousRaftOp can fail.
                    // The simple fix, is to ignore exception.
                } else {
                    throw e;
                }
            }
        }
        CPMap<String, Long> cpMap = instances[0].getCPSubsystem().getMap(name);
        assertThat(cpMap.get(key)).isGreaterThanOrEqualTo(increments.get());
        assertThat(cpMap.get(key)).isLessThanOrEqualTo(increments.get() + indeterminate.get());
    }

    private class Adder implements Runnable {
        private final HazelcastInstance instance;

        Adder(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            CPMap<String, Long> cpMap = instance.getCPSubsystem().getMap(name);
            while (!done.get()) {
                Long value = cpMap.get(key);
                try {
                    if (cpMap.compareAndSet(key, value, value + 1)) {
                        increments.incrementAndGet();
                    }
                } catch (IndeterminateOperationStateException | OperationTimeoutException e) {
                    indeterminate.incrementAndGet();
                }
            }
        }
    }
}
