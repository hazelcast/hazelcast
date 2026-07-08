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

package com.hazelcast.vector.internal.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.VectorCollectionMergeTypes;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.internal.impl.VectorTestUtils;
import com.hazelcast.vector.internal.impl.proxy.VectorCollectionProxyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatComparable;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionSplitBrainMergeTest extends SplitBrainTestSupport {

    private static final int INITIAL_ITEM_COUNT = 100;
    private static final int ADDITIONAL_ITEM_COUNT = 100;

    @Parameterized.Parameters(name = "mergePolicy:{0}, backupCount:{1}")
    public static Collection<Object[]> parameters() {
        return cartesianProduct(
                List.of(
                        DiscardMergePolicy.class.getName(),
                        PassThroughMergePolicy.class.getName(),
                        PutIfAbsentMergePolicy.class.getName(),
                        MergeOddNumbersPolicy.class.getName()
                ),
                List.of(0, 1)
        );
    }

    @Parameterized.Parameter
    public String mergePolicyClassName;

    @Parameterized.Parameter(1)
    public int backupCount;

    @Override
    protected Config config() {
        var config = super.config();
        var vcConfig = new VectorCollectionConfig(getCollectionName())
                .setBackupCount(backupCount)
                .addVectorIndexConfig(new VectorIndexConfig("index", Metric.COSINE, 2));
        vcConfig.setMergePolicyConfig(new MergePolicyConfig(mergePolicyClassName, 45));
        config.addVectorCollectionConfig(vcConfig);
        return config;
    }

    protected String getCollectionName() {
        return "test-"
                + mergePolicyClassName.replace('.', '_').replace('$', '_') + "-" + backupCount;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        super.onBeforeSplitBrainCreated(instances);
        VectorCollection<Integer, Integer> vectorCollection = instances[0].getVectorCollection(getCollectionName());
        Pipelining<Void> pipelining = new Pipelining<>(24);
        for (int i = 0; i < INITIAL_ITEM_COUNT; i++) {
            pipelining.add(vectorCollection.setAsync(i, VectorDocument.of(i, VectorTestUtils.randomVec(2))));
        }
        pipelining.results();
        assertEquals(INITIAL_ITEM_COUNT, vectorCollection.size());
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
        super.onAfterSplitBrainCreated(firstBrain, secondBrain);
        // add more entries in the smaller subcluster
        VectorCollection<Integer, Integer> vectorCollection = secondBrain[0].getVectorCollection(getCollectionName());
        for (int i = INITIAL_ITEM_COUNT; i < INITIAL_ITEM_COUNT + ADDITIONAL_ITEM_COUNT; i++) {
            assertThat(vectorCollection.setAsync(i, VectorDocument.of(i, VectorTestUtils.randomVec(2))))
                    .succeedsWithin(VectorCollectionProxyTest.TIMEOUT);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        super.onAfterSplitBrainHealed(instances);
        VectorCollection<Integer, Integer> vectorCollection = instances[0].getVectorCollection(getCollectionName());
        assertVectorCollectionSize(vectorCollection);
        if (backupCount > 0) {
            // ensure split-brain merge backups work: terminate a member and rerun assertions
            instances[instances.length - 1].getLifecycleService().terminate();
            waitClusterForSafeState(instances[0]);
            assertVectorCollectionSize(vectorCollection);
        }
    }

    private void assertVectorCollectionSize(VectorCollection<Integer, Integer> vectorCollection) {
        if (DiscardMergePolicy.class.getName().equals(mergePolicyClassName)) {
            assertWhenDiscardMergePolicy(vectorCollection);
        } else if (MergeOddNumbersPolicy.class.getName().equals(mergePolicyClassName)) {
            assertWithCustomMergePolicy(vectorCollection);
        } else {
            // with put-if-absent and pass-through policies, everything from the smaller cluster
            // is merged to the merged cluster
            assertEquals(INITIAL_ITEM_COUNT + ADDITIONAL_ITEM_COUNT, vectorCollection.size());
        }
    }

    private static void assertWithCustomMergePolicy(VectorCollection<Integer, Integer> vectorCollection) {
        // odd keys > 100 are discarded by the merge policy
        assertEquals(INITIAL_ITEM_COUNT + ADDITIONAL_ITEM_COUNT / 2, vectorCollection.size());
        // additional content assertions for custom merge policy: all keys >= 100 originate from the smaller cluster,
        // so their values must be 1000+key
        for (int i = INITIAL_ITEM_COUNT; i < INITIAL_ITEM_COUNT + ADDITIONAL_ITEM_COUNT; i += 2) {
            assertThat(vectorCollection.getAsync(i)).succeedsWithin(VectorCollectionProxyTest.TIMEOUT)
                    .extracting(VectorDocument::getValue)
                    .isEqualTo(1000 + i);
        }
        for (int i = INITIAL_ITEM_COUNT + 1; i < INITIAL_ITEM_COUNT + ADDITIONAL_ITEM_COUNT; i += 2) {
            assertThat(vectorCollection.getAsync(i)).succeedsWithin(VectorCollectionProxyTest.TIMEOUT)
                    .isNull();
        }
    }

    private void assertWhenDiscardMergePolicy(VectorCollection<Integer, Integer> vectorCollection) {
        if (backupCount == 0) {
            // With DiscardMergePolicy, all entries on the smaller subcluster are discarded
            // so we do not expect to find any of the additional entries added to the smaller subcluster.
            // Also, we expect that a portion (roughly 1/3rd) of the initially loaded items will be missing,
            // since they belonged to partitions owned by the single split member in the second brain and
            // no backups were configured.
            assertThatComparable(vectorCollection.size()).isLessThan((long) INITIAL_ITEM_COUNT);
        } else {
            // with backupCount > 0, items owned by the split member are promoted from backups
            // so we expect all the initial entries to be present
            assertThatComparable(vectorCollection.size()).isEqualTo((long) INITIAL_ITEM_COUNT);
        }
    }

    // Merge policy that exercises:
    // - returning either VectorDocument<Data> or VectorDocument<?>
    // - updating values of some keys: adds 1000 to the value of even keys >= 100
    // - deleting some entries: odd keys >= 100 are deleted
    public static class MergeOddNumbersPolicy implements SplitBrainMergePolicy<VectorDocument<Integer>,
            VectorCollectionMergeTypes<Integer, VectorDocument<Integer>>, Object> {

        @Override
        public Object merge(VectorCollectionMergeTypes<Integer, VectorDocument<Integer>> mergingValue,
                            VectorCollectionMergeTypes<Integer, VectorDocument<Integer>> existingValue) {
            var key = mergingValue.getKey();
            if (key < INITIAL_ITEM_COUNT) {
                // pass through all initial keys
                return mergingValue.getRawValue();
            }
            if (mergingValue.getKey() % 2 == 0) {
                // update value of even keys >= INITIAL_ITEM_COUNT that exist only in smaller cluster
                VectorDocument<Integer> deserializedMergingValue = mergingValue.getDeserializedValue();
                return VectorDocument.of(deserializedMergingValue.getValue() + 1000,
                        deserializedMergingValue.getVectors());
            } else {
                // delete odd keys >= INITIAL_ITEM_COUNT that exist only in smaller cluster
                return null;
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
