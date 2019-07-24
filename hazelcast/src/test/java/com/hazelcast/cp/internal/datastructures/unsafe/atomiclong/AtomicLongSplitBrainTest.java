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

package com.hazelcast.cp.internal.datastructures.unsafe.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicLongMergeTypes;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.cp.internal.datastructures.unsafe.ConcurrencyTestUtil.getAtomicLongBackup;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link IAtomicLong}.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicLongSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {DiscardMergePolicy.class},
                {PassThroughMergePolicy.class},
                {PutIfAbsentMergePolicy.class},
                {RemoveValuesMergePolicy.class},
                {ReturnLongPiMergePolicy.class},
                {MergeGreaterValueMergePolicy.class},
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private String atomicLongNameA = randomMapName("atomicLongA-");
    private String atomicLongNameB = randomMapName("atomicLongB-");
    private IAtomicLong atomicLongA1;
    private IAtomicLong atomicLongA2;
    private IAtomicLong atomicLongB1;
    private IAtomicLong atomicLongB2;
    private AtomicLong backupAtomicLongA;
    private AtomicLong backupAtomicLongB;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getAtomicLongConfig(atomicLongNameA)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getAtomicLongConfig(atomicLongNameB)
                .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        atomicLongA1 = firstBrain[0].getAtomicLong(atomicLongNameA);
        atomicLongA2 = secondBrain[0].getAtomicLong(atomicLongNameA);
        atomicLongB2 = secondBrain[0].getAtomicLong(atomicLongNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterSplitPutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RemoveValuesMergePolicy.class) {
            afterSplitRemoveValuesMergePolicy();
        } else if (mergePolicyClass == ReturnLongPiMergePolicy.class) {
            afterSplitReturnLongPiMergePolicy();
        } else if (mergePolicyClass == MergeGreaterValueMergePolicy.class) {
            afterSplitCustomMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        atomicLongB1 = instances[0].getAtomicLong(atomicLongNameB);

        backupAtomicLongA = getAtomicLongBackup(instances, atomicLongA1);
        backupAtomicLongB = getAtomicLongBackup(instances, atomicLongB1);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterMergePutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RemoveValuesMergePolicy.class) {
            afterMergeRemoveValuesMergePolicy();
        } else if (mergePolicyClass == ReturnLongPiMergePolicy.class) {
            afterMergeReturnLongPiMergePolicy();
        } else if (mergePolicyClass == MergeGreaterValueMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        atomicLongA1.set(23);

        atomicLongA2.set(42);
        atomicLongB2.set(43);
    }

    private void afterMergeDiscardMergePolicy() {
        assertEquals(23, atomicLongA1.get());
        assertEquals(23, atomicLongA2.get());
        assertEquals(23, backupAtomicLongA.get());

        assertEquals(0, atomicLongB1.get());
        assertEquals(0, atomicLongB2.get());
        assertEquals(0, backupAtomicLongB.get());
    }

    private void afterSplitPassThroughMergePolicy() {
        atomicLongA1.set(23);

        atomicLongA2.set(42);
        atomicLongB2.set(43);
    }

    private void afterMergePassThroughMergePolicy() {
        assertEquals(42, atomicLongA1.get());
        assertEquals(42, atomicLongA2.get());
        assertEquals(42, backupAtomicLongA.get());

        assertEquals(43, atomicLongB1.get());
        assertEquals(43, atomicLongB2.get());
        assertEquals(43, backupAtomicLongB.get());
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        atomicLongA1.set(23);

        atomicLongA2.set(42);
        atomicLongB2.set(43);
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertEquals(23, atomicLongA1.get());
        assertEquals(23, atomicLongA2.get());
        assertEquals(23, backupAtomicLongA.get());

        assertEquals(43, atomicLongB1.get());
        assertEquals(43, atomicLongB2.get());
        assertEquals(43, backupAtomicLongB.get());
    }

    private void afterSplitRemoveValuesMergePolicy() {
        atomicLongA1.set(23);

        atomicLongA2.set(42);
        atomicLongB2.set(43);
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertEquals(0, atomicLongA1.get());
        assertEquals(0, atomicLongA2.get());
        assertEquals(0, backupAtomicLongA.get());

        assertEquals(0, atomicLongB1.get());
        assertEquals(0, atomicLongB2.get());
        assertEquals(0, backupAtomicLongB.get());
    }

    private void afterSplitReturnLongPiMergePolicy() {
        atomicLongA1.set(23);

        atomicLongA2.set(42);
        atomicLongB2.set(43);
    }

    private void afterMergeReturnLongPiMergePolicy() {
        assertEquals(ReturnLongPiMergePolicy.LONG_PI, atomicLongA1.get());
        assertEquals(ReturnLongPiMergePolicy.LONG_PI, atomicLongA2.get());
        assertEquals(ReturnLongPiMergePolicy.LONG_PI, backupAtomicLongA.get());

        assertEquals(ReturnLongPiMergePolicy.LONG_PI, atomicLongB1.get());
        assertEquals(ReturnLongPiMergePolicy.LONG_PI, atomicLongB2.get());
        assertEquals(ReturnLongPiMergePolicy.LONG_PI, backupAtomicLongB.get());
    }

    private void afterSplitCustomMergePolicy() {
        atomicLongA1.set(23);

        atomicLongA2.set(42);
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(42, atomicLongA1.get());
        assertEquals(42, atomicLongA2.get());
        assertEquals(42, backupAtomicLongA.get());
    }

    private static class ReturnLongPiMergePolicy implements SplitBrainMergePolicy<Long, AtomicLongMergeTypes> {

        private static final long LONG_PI = 31415L;

        @Override
        public Long merge(AtomicLongMergeTypes mergingValue, AtomicLongMergeTypes existingValue) {
            return LONG_PI;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    private static class MergeGreaterValueMergePolicy implements SplitBrainMergePolicy<Long, AtomicLongMergeTypes> {

        @Override
        public Long merge(AtomicLongMergeTypes mergingValue, AtomicLongMergeTypes existingValue) {
            if (mergingValue.getValue() > existingValue.getValue()) {
                return mergingValue.getValue();
            }
            return existingValue.getValue();
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }
}
