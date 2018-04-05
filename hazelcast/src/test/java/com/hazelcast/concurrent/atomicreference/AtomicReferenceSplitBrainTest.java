/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.concurrent.ConcurrencyTestUtil.getAtomicReferenceBackup;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link IAtomicReference}.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicReferenceSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {DiscardMergePolicy.class},
                {PassThroughMergePolicy.class},
                {PutIfAbsentMergePolicy.class},
                {MergeInstanceOfIntegerMergePolicy.class},
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private String atomicReferenceNameA = randomMapName("atomicReferenceA-");
    private String atomicReferenceNameB = randomMapName("atomicReferenceB-");
    private IAtomicReference<Object> atomicReferenceA1;
    private IAtomicReference<Object> atomicReferenceA2;
    private IAtomicReference<Object> atomicReferenceB1;
    private IAtomicReference<Object> atomicReferenceB2;
    private AtomicReference<Object> backupAtomicReferenceA;
    private AtomicReference<Object> backupAtomicReferenceB;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getAtomicReferenceConfig(atomicReferenceNameA)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getAtomicReferenceConfig(atomicReferenceNameB)
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

        atomicReferenceA1 = firstBrain[0].getAtomicReference(atomicReferenceNameA);
        atomicReferenceA2 = secondBrain[0].getAtomicReference(atomicReferenceNameA);
        atomicReferenceB2 = secondBrain[0].getAtomicReference(atomicReferenceNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterSplitPutIfAbsentMergePolicy();
        } else if (mergePolicyClass == MergeInstanceOfIntegerMergePolicy.class) {
            afterSplitCustomMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        atomicReferenceB1 = instances[0].getAtomicReference(atomicReferenceNameB);
        backupAtomicReferenceA = getAtomicReferenceBackup(instances, atomicReferenceA1);
        backupAtomicReferenceB = getAtomicReferenceBackup(instances, atomicReferenceB1);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterMergePutIfAbsentMergePolicy();
        } else if (mergePolicyClass == MergeInstanceOfIntegerMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        atomicReferenceA1.set(23);

        atomicReferenceA2.set(42);
        atomicReferenceB2.set(43);
    }

    private void afterMergeDiscardMergePolicy() {
        assertEquals(23, atomicReferenceA1.get());
        assertEquals(23, atomicReferenceA2.get());
        assertEquals(23, backupAtomicReferenceA.get());

        assertNull(atomicReferenceB1.get());
        assertNull(atomicReferenceB2.get());
        assertNull(backupAtomicReferenceB.get());
    }

    private void afterSplitPassThroughMergePolicy() {
        atomicReferenceA1.set(23);

        atomicReferenceA2.set(42);
        atomicReferenceB2.set(43);
    }

    private void afterMergePassThroughMergePolicy() {
        assertEquals(42, atomicReferenceA1.get());
        assertEquals(42, atomicReferenceA2.get());
        assertEquals(42, backupAtomicReferenceA.get());

        assertEquals(43, atomicReferenceB1.get());
        assertEquals(43, atomicReferenceB2.get());
        assertEquals(43, backupAtomicReferenceB.get());
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        atomicReferenceA1.set(23);

        atomicReferenceA2.set(42);
        atomicReferenceB2.set(43);
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertEquals(23, atomicReferenceA1.get());
        assertEquals(23, atomicReferenceA2.get());
        assertEquals(23, backupAtomicReferenceA.get());

        assertEquals(43, atomicReferenceB1.get());
        assertEquals(43, atomicReferenceB2.get());
        assertEquals(43, backupAtomicReferenceB.get());
    }

    private void afterSplitCustomMergePolicy() {
        atomicReferenceA1.set(42);

        atomicReferenceA2.set("23");
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(42, atomicReferenceA1.get());
        assertEquals(42, atomicReferenceA2.get());
        assertEquals(42, backupAtomicReferenceA.get());
    }

    private static class MergeInstanceOfIntegerMergePolicy implements SplitBrainMergePolicy {

        @Override
        public <T> T merge(MergingValue<T> mergingValue, MergingValue<T> existingValue) {
            if (mergingValue.getValue() instanceof Integer) {
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
