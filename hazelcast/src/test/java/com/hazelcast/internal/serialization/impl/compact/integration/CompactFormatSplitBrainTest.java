/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.integration;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.NodeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.assertSchemasAvailable;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactFormatSplitBrainTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private Brain largerBrain;
    private Brain smallerBrain;
    private List<HazelcastInstance> allInstances;

    @Before
    public void setUp() {
        Config config = getMemberConfig();

        largerBrain = new Brain(Arrays.asList(
                factory.newHazelcastInstance(config),
                factory.newHazelcastInstance(config),
                factory.newHazelcastInstance(config)
        ));

        smallerBrain = new Brain(Arrays.asList(
                factory.newHazelcastInstance(config),
                factory.newHazelcastInstance(config)
        ));

        allInstances = new ArrayList<>();
        allInstances.addAll(largerBrain.instances);
        allInstances.addAll(smallerBrain.instances);

        assertClusterSizeEventually(allInstances.size(), allInstances);
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testSplitBrainHealing_whenSmallerClusterHasNoSchemas() {
        splitCluster();

        // Schemas of the Employee and the Node classes are only available in the larger brain
        fillEmployeeMapUsing(largerBrain.getRandomInstance());
        fillNodeMapUsing(largerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(smallerBrain.getRandomInstance());
        assertNodeMapSizeUsing(smallerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(smallerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(smallerBrain.getRandomInstance());
    }

    @Test
    public void testSplitBrainHealing_whenLargeClusterHasNoSchemas() {
        splitCluster();

        // Schemas of the Employee and the Node classes are only available in the smaller brain
        fillEmployeeMapUsing(smallerBrain.getRandomInstance());
        fillNodeMapUsing(smallerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(largerBrain.getRandomInstance());
        assertNodeMapSizeUsing(largerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(largerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(largerBrain.getRandomInstance());
    }

    @Test
    public void testSplitBrainHealing_whenBothClusterHaveSameSchemas() {
        splitCluster();

        // Schemas of the Employee and the Node classes are only available in both of the brains
        fillEmployeeMapUsing(largerBrain.getRandomInstance());
        fillEmployeeMapUsing(smallerBrain.getRandomInstance());

        fillNodeMapUsing(largerBrain.getRandomInstance());
        fillNodeMapUsing(smallerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(largerBrain.getRandomInstance());
        assertNodeMapSizeUsing(smallerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(smallerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(largerBrain.getRandomInstance());
    }

    @Test
    public void testSplitBrainHealing_whenBothClusterHaveDifferentSchemas() {
        splitCluster();

        // The schema of the Employee class is only available in the larger brain
        fillEmployeeMapUsing(largerBrain.getRandomInstance());

        // The schema of the Node class is only available in the smaller brain
        fillNodeMapUsing(smallerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(smallerBrain.getRandomInstance());
        assertNodeMapSizeUsing(largerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(smallerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(largerBrain.getRandomInstance());
    }

    private static class Brain {
        private final List<HazelcastInstance> instances;
        private final Random random = new Random();

        private Brain(List<HazelcastInstance> instances) {
            this.instances = instances;
        }

        public HazelcastInstance getRandomInstance() {
            return instances.get(random.nextInt(instances.size()));
        }

        public void splitFrom(Brain other) {
            List<HazelcastInstance> otherBrainInstances = other.instances;
            for (HazelcastInstance instance : instances) {
                for (HazelcastInstance otherBrainInstance : otherBrainInstances) {
                    blockCommunicationBetween(instance, otherBrainInstance);
                }
            }

            for (HazelcastInstance instance : instances) {
                for (HazelcastInstance otherBrainInstance : otherBrainInstances) {
                    closeConnectionBetween(instance, otherBrainInstance);
                }
            }
        }

        public void mergeWith(Brain other) {
            for (HazelcastInstance instance : instances) {
                for (HazelcastInstance otherBrainInstance : other.instances) {
                    unblockCommunicationBetween(instance, otherBrainInstance);
                }
            }
        }
    }

    private void splitCluster() {
        largerBrain.splitFrom(smallerBrain);

        // make sure that cluster is split
        assertClusterSizeEventually(largerBrain.instances.size(), largerBrain.instances);
        assertClusterSizeEventually(smallerBrain.instances.size(), smallerBrain.instances);
        waitAllForSafeState(allInstances);
    }

    private void mergeCluster() {
        largerBrain.mergeWith(smallerBrain);

        assertClusterSizeEventually(allInstances.size(), allInstances);
        waitAllForSafeState(allInstances);
    }

    private Config getMemberConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig("employeeMap")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.getMapConfig("nodeMap")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        return config;
    }

    private void assertSchemasAvailableInEveryMember() {
        assertSchemasAvailable(allInstances, EmployeeDTO.class, NodeDTO.class);
    }

    private void fillEmployeeMapUsing(HazelcastInstance instance) {
        IMap<Integer, EmployeeDTO> employeeMap = instance.getMap("employeeMap");
        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = new EmployeeDTO(i, i);
            employeeMap.put(i, employeeDTO);
        }
    }

    private void fillNodeMapUsing(HazelcastInstance instance) {
        IMap<Integer, NodeDTO> nodeMap = instance.getMap("nodeMap");
        for (int i = 0; i < 100; i++) {
            NodeDTO node = new NodeDTO(new NodeDTO(null, i), i);
            nodeMap.put(i, node);
        }
    }

    private void assertEmployeeMapSizeUsing(HazelcastInstance instance) {
        IMap<Integer, EmployeeDTO> employeeMap = instance.getMap("employeeMap");
        assertTrueEventually(() -> assertEquals(100, employeeMap.size()));
    }

    private void assertNodeMapSizeUsing(HazelcastInstance instance) {
        IMap<Integer, NodeDTO> nodeMap = instance.getMap("nodeMap");
        assertTrueEventually(() -> assertEquals(100, nodeMap.size()));
    }

    private void assertQueryForEmployeeMapUsing(HazelcastInstance instance) {
        IMap<Integer, EmployeeDTO> employeeMap = instance.getMap("employeeMap");
        int size = employeeMap.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size);
    }

    private void assertQueryForNodeMapUsing(HazelcastInstance instance) {
        IMap<Integer, NodeDTO> nodeMap = instance.getMap("nodeMap");
        int size = nodeMap.keySet(Predicates.sql("child.id > 19")).size();
        assertEquals(80, size);
    }
}
