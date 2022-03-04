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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.test.AssertTask;
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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Tests different split-brain scenarios for {@link PNCounter}.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "replicaCount:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1},
                {2},
                {Integer.MAX_VALUE},
        });
    }

    @Parameter
    public int replicaCount;

    private String counterName = randomMapName("counter-");
    private PNCounter counter1;
    private PNCounter counter2;
    private MergeLifecycleListener mergeLifecycleListener;
    private AtomicLong assertCounter = new AtomicLong();

    @Override
    protected Config config() {
        Config config = super.config();
        config.getPNCounterConfig(counterName)
                .setReplicaCount(replicaCount);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        for (HazelcastInstance instance : instances) {
            final int delta = 100;
            getCounter(instance).addAndGet(delta);
            assertCounter.addAndGet(delta);
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        counter1 = getCounter(firstBrain[0]);
        counter2 = getCounter(secondBrain[0]);

        counter1.addAndGet(100);
        assertCounter.addAndGet(100);
        counter2.addAndGet(100);
        assertCounter.addAndGet(100);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        assertCounterValueEventually(assertCounter.get(), counter1);
        assertCounterValueEventually(assertCounter.get(), counter2);
    }

    private PNCounter getCounter(HazelcastInstance instance) {
        final PNCounter pnCounter = instance.getPNCounter(counterName);
        ((PNCounterProxy) pnCounter).setOperationTryCount(1);
        pnCounter.reset();
        return pnCounter;
    }

    private void assertCounterValueEventually(final long expectedValue, final PNCounter counter) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedValue, counter.get());
            }
        });
    }
}
