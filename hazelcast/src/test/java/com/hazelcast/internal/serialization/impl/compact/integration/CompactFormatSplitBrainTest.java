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
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.NodeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.assertSchemasAvailable;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactFormatSplitBrainTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    private HazelcastInstance instance3;
    private HazelcastInstance instance4;
    private HazelcastInstance instance5;

    private List<HazelcastInstance> splitA;
    private List<HazelcastInstance> splitB;


    @Before
    public void setUp() {
        Config config = getMemberConfig();
        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        instance3 = factory.newHazelcastInstance(config);
        instance4 = factory.newHazelcastInstance(config);
        instance5 = factory.newHazelcastInstance(config);

        splitA = Arrays.asList(instance1, instance2, instance3);
        splitB = Arrays.asList(instance4, instance5);
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testSplitBrainHealing_whenSmallerClusterHasNoSchemas() {
        splitCluster();

        // The data is only available in the larger cluster
        fillEmployeeMapUsing(instance1);
        fillNodeMapUsing(instance1);

        healCluster();

        assertEmployeeMapSizeUsing(instance5);
        assertNodeMapSizeUsing(instance5);

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(instance5);
        assertQueryForNodeMapUsing(instance5);
    }

    @Test
    public void testSplitBrainHealing_whenLargeClusterHasNoSchemas() {
        splitCluster();

        // The data is only available in the smaller cluster
        fillEmployeeMapUsing(instance5);
        fillNodeMapUsing(instance5);

        healCluster();

        assertEmployeeMapSizeUsing(instance1);
        assertNodeMapSizeUsing(instance1);

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(instance1);
        assertQueryForNodeMapUsing(instance1);
    }

    @Test
    public void testSplitBrainHealing_whenBothClusterHaveSameSchemas() {
        splitCluster();

        // The data is available in both clusters
        fillEmployeeMapUsing(instance1);
        fillEmployeeMapUsing(instance5);

        fillNodeMapUsing(instance1);
        fillNodeMapUsing(instance5);

        healCluster();

        assertEmployeeMapSizeUsing(instance1);
        assertNodeMapSizeUsing(instance5);

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(instance5);
        assertQueryForNodeMapUsing(instance1);
    }

    @Test
    public void testSplitBrainHealing_whenBothClusterHaveDifferentSchemas() {
        splitCluster();

        // The data is only available in the large cluster
        fillEmployeeMapUsing(instance1);

        // Tha data is only available in the smaller cluster
        fillNodeMapUsing(instance5);

        healCluster();

        assertEmployeeMapSizeUsing(instance5);
        assertNodeMapSizeUsing(instance1);

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(instance5);
        assertQueryForNodeMapUsing(instance1);
    }

    private void splitCluster() {
        for (HazelcastInstance splitAInstance : splitA) {
            for (HazelcastInstance splitBInstance : splitB) {
                SplitBrainTestSupport.blockCommunicationBetween(splitAInstance, splitBInstance);
                closeConnectionBetween(splitAInstance, splitBInstance);
            }
        }

        // make sure that cluster is split as [1 , 2, 3] , [4, 5]
        assertClusterSizeEventually(3, instance1, instance2, instance3);
        assertClusterSizeEventually(2, instance4, instance5);
    }

    private void healCluster() {
        for (HazelcastInstance splitAInstance : splitA) {
            for (HazelcastInstance splitBInstance : splitB) {
                SplitBrainTestSupport.unblockCommunicationBetween(splitAInstance, splitBInstance);
            }
        }

        assertClusterSizeEventually(5, instance1, instance2, instance3, instance4, instance5);
    }

    private Config getMemberConfig() {
        Config config = smallInstanceConfig();
        config.getMapConfig("employeeMap")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.getMapConfig("nodeMap")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.getSerializationConfig()
                .getCompactSerializationConfig()
                .setEnabled(true);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        return config;
    }

    private void assertSchemasAvailableInEveryMember() {
        Collection<HazelcastInstance> instances = Arrays.asList(
                instance1,
                instance2,
                instance3,
                instance4,
                instance5
        );
        assertSchemasAvailable(instances, EmployeeDTO.class, NodeDTO.class);
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
