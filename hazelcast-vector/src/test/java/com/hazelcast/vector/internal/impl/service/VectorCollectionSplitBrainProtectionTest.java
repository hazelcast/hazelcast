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

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.vector.internal.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionSplitBrainProtectionTest extends HazelcastTestSupport {

    private VectorCollection<Integer, Integer> collection;

    @Test
    public void whenSplitBrainProtectionOnReadAndMembersLeave_thenReadOperationsFail() {
        var config = smallInstanceConfig();
        config.getSplitBrainProtectionConfig("atLeastThreeMembers").setMinimumClusterSize(3).setEnabled(true);
        config.addVectorCollectionConfig(new VectorCollectionConfig("test")
                .addVectorIndexConfig(new VectorIndexConfig("test", Metric.EUCLIDEAN, 2))
                .setSplitBrainProtectionName("atLeastThreeMembers"));
        var members = createHazelcastInstances(config, 3);
        collection = members[0].getVectorCollection("test");

        // while cluster has 3 members, VectorCollection operates normally
        assertThat(collection.putAsync(1, VectorDocument.of(1, VectorValues.of(new float[] {1, 1}))))
                .succeedsWithin(TIMEOUT).isNull();
        assertThat(collection.getAsync(1)).succeedsWithin(TIMEOUT).isNotNull();
        assertThat(collection.searchAsync(VectorValues.of(new float[] {1, 1}), SearchOptions.builder().limit(10).build())).succeedsWithin(TIMEOUT).isNotNull();

        // shutdown member
        members[1].shutdown();
        assertClusterSizeEventually(2, members[0]);
        testOperationsForReadProtection();

        // shutdown another member
        // (search uses different default implementation on 1-member cluster)
        members[2].shutdown();
        testOperationsForReadProtection();
    }

    private void testOperationsForReadProtection() {
        assertThat(collection.getAsync(1)).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
        assertThat(collection.searchAsync(VectorValues.of(new float[] {1, 1}), SearchOptions.builder().limit(10).build()))
                .failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void whenSplitBrainProtectionOnWriteAndMembersLeave_thenReadSucceeds() {
        var config = smallInstanceConfig();
        config.getSplitBrainProtectionConfig("atLeastThreeMembers")
                .setMinimumClusterSize(3).setEnabled(true).setProtectOn(SplitBrainProtectionOn.WRITE);

        config.addVectorCollectionConfig(new VectorCollectionConfig("test")
                .addVectorIndexConfig(new VectorIndexConfig("test", Metric.EUCLIDEAN, 2))
                .setSplitBrainProtectionName("atLeastThreeMembers"));
        var members = createHazelcastInstances(config, 3);
        collection = members[0].getVectorCollection("test");

        // while cluster has 3 members, VectorCollection operates normally
        assertThat(collection.putAsync(1, VectorDocument.of(1, VectorValues.of(new float[] {1, 1}))))
                .succeedsWithin(TIMEOUT).isNull();
        assertThat(collection.getAsync(1)).succeedsWithin(TIMEOUT).isNotNull();

        // shutdown member
        members[1].shutdown();
        assertClusterSizeEventually(2, members[0]);
        testOperationsForWriteProtection();

        // shutdown another member
        // (search uses different default implementation on 1-member cluster)
        members[2].shutdown();
        testOperationsForWriteProtection();
    }

    private void testOperationsForWriteProtection() {
        // read-only operations succeed
        assertThat(collection.getAsync(1)).succeedsWithin(TIMEOUT).isNotNull();
        assertThat(collection.searchAsync(VectorValues.of(new float[] {1, 1}), SearchOptions.builder().build())).succeedsWithin(TIMEOUT).isNotNull();

        // write fails
        assertThat(collection.putAsync(2, VectorDocument.of(2, VectorValues.of(new float[] {1, 1}))))
                .failsWithin(TIMEOUT).withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }
}
