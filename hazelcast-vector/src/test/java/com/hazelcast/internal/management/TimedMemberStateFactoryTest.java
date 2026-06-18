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

package com.hazelcast.internal.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.VectorCollection;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.Supplier;

import static com.hazelcast.test.Accessors.getHazelcastInstanceImpl;
import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.HazelcastTestSupport.waitClusterForSafeState;
import static com.hazelcast.vector.impl.VectorTestUtils.warmupOneIndexCollection;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TimedMemberStateFactoryTest {

    TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void teardown() {
        factory.shutdownAll();
    }

    @Test
    public void testVectorCollections_whenNoCollection_thenStateIsEmpty() {
        HazelcastInstance hz = factory.newHazelcastInstance(smallInstanceConfig());
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        assertThat(timedMemberState.getMemberState().getVectorCollections()).isEmpty();
    }

    @Test
    public void testVectorCollections_whenVectorCollectionStorageExists() {
        String vectorCollectionName = randomString();

        HazelcastInstance[] instances = factory.newInstances(
            smallInstanceConfig(), 3);

        // when VectorCollection is created and data exist on all partitions
        createAndPopulateVectorCollection(vectorCollectionName, instances[0]);

        // then VectorCollection name is reported in all members' MemberState
        for (HazelcastInstance hz : instances) {
            TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
            TimedMemberState timedMemberState = factory.createTimedMemberState();
            assertThat(timedMemberState.getMemberState().getVectorCollections())
                .containsExactly(vectorCollectionName);
        }
    }

    @Test
    public void testVectorCollections_whenVectorCollectionProxyExists() {
        String vectorCollectionName = randomString();

        HazelcastInstance[] instances = factory.newInstances(
            smallInstanceConfig(), 3);
        // ensure that all members see each other to avoid race condition when member joins late
        assertClusterSizeEventually(3, instances);

        // when VectorCollection is created but no data exists
        createVectorCollection(vectorCollectionName, instances[0]);

        // then VectorCollection name is reported in all members' MemberState
        for (HazelcastInstance hz : instances) {
            TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
            assertTrueEventually(() -> {
                TimedMemberState timedMemberState = factory.createTimedMemberState();
                assertThat(timedMemberState.getMemberState().getVectorCollections())
                    .containsExactly(vectorCollectionName);
            });
        }
    }

    @Test
    public void testVectorCollections_whenVectorCollectionCleared() {
        String vectorCollectionName = randomString();

        HazelcastInstance[] instances = factory.newInstances(
            smallInstanceConfig(), 3);

        // when VectorCollection is created, all partitions populated and then cleared
        var vectorCollection = createAndPopulateVectorCollection(vectorCollectionName, instances[0]);
        assertThat(vectorCollection.clearAsync()).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        // then VectorCollection name is reported in all members' MemberState
        for (HazelcastInstance hz : instances) {
            TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
            TimedMemberState timedMemberState = factory.createTimedMemberState();
            assertThat(timedMemberState.getMemberState().getVectorCollections())
                .containsExactly(vectorCollectionName);
        }
    }

    @Test
    public void testVectorCollections_whenVectorCollectionDestroyed() {
        String vectorCollectionName = randomString();

        HazelcastInstance[] instances = factory.newInstances(
            smallInstanceConfig(), 3);
        assertClusterSizeEventually(3, instances);

        // when VectorCollection is created, all partitions populated and then destroyed
        var vectorCollection = createAndPopulateVectorCollection(vectorCollectionName, instances[0]);
        vectorCollection.destroy();

        // then VectorCollection name is not reported in any members' MemberState
        for (HazelcastInstance hz : instances) {
            TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
            assertTrueEventually(() -> {
                TimedMemberState timedMemberState = factory.createTimedMemberState();
                assertThat(timedMemberState.getMemberState().getVectorCollections()).isEmpty();
            });
        }
    }

    @Test
    public void testVectorCollections_whenVectorCollectionClearedAndMigrationsOccur() {
        String vectorCollectionName = randomString();
        Supplier<Config> configSupplier = HazelcastTestSupport::smallInstanceConfig;
        HazelcastInstance[] instances = factory.newInstances(configSupplier, 3);

        // when VectorCollection is created, all partitions populated then cleared and cluster undergoes rolling restart
        var vectorCollection = createAndPopulateVectorCollection(vectorCollectionName, instances[0]);
        assertThat(vectorCollection.clearAsync()).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        rollingRestart(instances, configSupplier);

        // then VectorCollection name is reported in all members' MemberState
        for (HazelcastInstance hz : instances) {
            TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
            TimedMemberState timedMemberState = factory.createTimedMemberState();
            assertThat(timedMemberState.getMemberState().getVectorCollections())
                .containsExactly(vectorCollectionName);
        }
    }

    @Test
    public void testVectorCollections_whenDataMemberDemoted() {
        String vectorCollectionName = randomString();

        HazelcastInstance[] instances = factory.newInstances(
            smallInstanceConfig(), 3);

        // when VectorCollection is created and data exist on all partitions but one member is demoted
        createAndPopulateVectorCollection(vectorCollectionName, instances[0]);
        instances[2].getCluster().demoteLocalDataMember();
        waitClusterForSafeState(instances[0]);

        // then VectorCollection name is still reported in all members' MemberState including lite member
        for (HazelcastInstance hz : instances) {
            TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
            TimedMemberState timedMemberState = factory.createTimedMemberState();
            assertThat(timedMemberState.getMemberState().getVectorCollections())
                .containsExactly(vectorCollectionName);
        }
    }

    private static VectorCollection<String, String> createAndPopulateVectorCollection(String vectorCollectionName,
                                                                                      HazelcastInstance instance) {
        VectorCollection<String, String> vc = createVectorCollection(vectorCollectionName, instance);
        warmupOneIndexCollection(instance, vc);
        return vc;
    }

    private static VectorCollection<String, String> createVectorCollection(String vectorCollectionName, HazelcastInstance instance) {
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(vectorCollectionName);
        vectorCollectionConfig.addVectorIndexConfig(new VectorIndexConfig("index-1", Metric.COSINE, 40));
        instance.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return instance.getVectorCollection(vectorCollectionConfig.getName());
    }

    private void rollingRestart(HazelcastInstance[] instances, Supplier<Config> configSupplier) {
        for (int i = 0; i < instances.length; i++) {
            instances[i].shutdown();
            waitClusterForSafeState(instances[(i + 1) % instances.length]);
            instances[i] = factory.newHazelcastInstance(configSupplier.get());
            waitClusterForSafeState(instances[(i + 1) % instances.length]);
        }
    }
}
