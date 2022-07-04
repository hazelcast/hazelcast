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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ReplicatedMapMergePolicyTest extends HazelcastTestSupport {

    @Parameters(name = "{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{
                new LatestUpdateMergePolicyTestCase(),
                new HighestHitsMergePolicyTestCase(),
                new PutIfAbsentMapMergePolicyTestCase(),
                new PassThroughMapMergePolicyTestCase(),
                new CustomMergePolicyTestCase(),
        });
    }

    @Parameter
    public ReplicatedMapMergePolicyTestCase testCase;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory(2);
    }

    @Test
    public void testMapMergePolicy() {
        final String mapName = randomMapName();
        Config config = newConfig(testCase.getMergePolicyClassName(), mapName);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);

        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        // wait for cluster to be formed before breaking the connection
        waitAllForSafeState(h1, h2);

        blockCommunicationBetween(h1, h2);
        closeConnectionBetween(h1, h2);

        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        ReplicatedMap<Object, Object> map1 = h1.getReplicatedMap(mapName);
        ReplicatedMap<Object, Object> map2 = h2.getReplicatedMap(mapName);
        final Map<Object, Object> expectedValues = testCase.populateMaps(map1, map2, h1);

        unblockCommunicationBetween(h1, h2);

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1, h2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ReplicatedMap<Object, Object> mapTest = h1.getReplicatedMap(mapName);
                for (Map.Entry<Object, Object> entry : expectedValues.entrySet()) {
                    assertEquals(entry.getValue(), mapTest.get(entry.getKey()));
                }
            }
        });
    }

    private Config newConfig(String mergePolicy, String mapName) {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.setClusterName(generateRandomString(10));
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        replicatedMapConfig.getMergePolicyConfig().setPolicy(mergePolicy);
        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        CountDownLatch latch;

        TestLifeCycleListener(int countdown) {
            latch = new CountDownLatch(countdown);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }
    }

    private interface ReplicatedMapMergePolicyTestCase {
        // Populate given maps with K-V pairs. Optional HZ instance is required by specific merge policies in order to
        // generate keys owned by the given instance.
        // return K-V pairs expected to be found in the merged map
        Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                         HazelcastInstance instance);

        // return merge policy's class name
        String getMergePolicyClassName();
    }

    private static class LatestUpdateMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HazelcastInstance instance) {
            map1.put("key1", "value");
            // prevent updating at the same time
            sleepAtLeastSeconds(1);
            map2.put("key1", "LatestUpdatedValue");
            map2.put("key2", "value2");
            // prevent updating at the same time
            sleepAtLeastSeconds(1);
            map1.put("key2", "LatestUpdatedValue2");

            Map<Object, Object> expectedValues = new HashMap<Object, Object>();
            expectedValues.put("key1", "LatestUpdatedValue");
            expectedValues.put("key2", "LatestUpdatedValue2");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return LatestUpdateMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "LatestUpdateMapMergePolicy";
        }
    }

    private static class HighestHitsMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HazelcastInstance instance) {
            map1.put("key1", "higherHitsValue");
            map1.put("key2", "value2");
            // increase hits number
            map1.get("key1");
            map1.get("key1");

            map2.put("key1", "value1");
            map2.put("key2", "higherHitsValue2");
            // increase hits number
            map2.get("key2");
            map2.get("key2");

            Map<Object, Object> expectedValues = new HashMap<Object, Object>();
            expectedValues.put("key1", "higherHitsValue");
            expectedValues.put("key2", "higherHitsValue2");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return HigherHitsMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "HigherHitsMapMergePolicy";
        }
    }

    private static class PutIfAbsentMapMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HazelcastInstance instance) {
            map1.put("key1", "PutIfAbsentValue1");

            map2.put("key1", "value");
            map2.put("key2", "PutIfAbsentValue2");

            Map<Object, Object> expectedValues = new HashMap<Object, Object>();
            expectedValues.put("key1", "PutIfAbsentValue1");
            expectedValues.put("key2", "PutIfAbsentValue2");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return PutIfAbsentMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "PutIfAbsentMapMergePolicy";
        }
    }

    private static class PassThroughMapMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HazelcastInstance instance) {
            assertNotNull(instance);
            String key = generateKeyOwnedBy(instance);
            map1.put(key, "value");

            map2.put(key, "passThroughValue");
            Map<Object, Object> expectedValues = new HashMap<Object, Object>();
            expectedValues.put(key, "passThroughValue");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return PassThroughMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "PassThroughMergePolicy";
        }
    }

    private static class CustomMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HazelcastInstance instance) {
            assertNotNull(instance);
            String key = generateKeyOwnedBy(instance);
            Integer value = 1;
            map1.put(key, "value");

            map2.put(key, value);

            Map<Object, Object> expectedValues = new HashMap<Object, Object>();
            expectedValues.put(key, value);
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return CustomReplicatedMapMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "CustomMergePolicy";
        }
    }
}
