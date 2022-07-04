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
import com.hazelcast.config.CompactSerializationConfig;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactFormatSplitBrainTest extends HazelcastTestSupport {

    TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testSchemaAccessibleAfterMergingClusters() {
        Config config = smallInstanceConfig();
        config.getMapConfig("map1")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.getMapConfig("map3")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        SplitBrainTestSupport.blockCommunicationBetween(instance1, instance3);
        closeConnectionBetween(instance1, instance3);
        SplitBrainTestSupport.blockCommunicationBetween(instance2, instance3);
        closeConnectionBetween(instance2, instance3);

        // make sure that cluster is split as [ 1 , 2 ] , [ 3 ]
        assertClusterSizeEventually(2, instance1, instance2);
        assertClusterSizeEventually(1, instance3);

        IMap<Integer, EmployeeDTO> map1 = instance1.getMap("map1");
        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = new EmployeeDTO(i, 102310312);
            map1.put(i, employeeDTO);
        }

        IMap<Integer, NodeDTO> map3 = instance3.getMap("map3");
        for (int i = 0; i < 100; i++) {
            NodeDTO node = new NodeDTO(new NodeDTO(null, i), i);
            map3.put(i, node);
        }

        assertEquals(100, map1.size());
        assertEquals(100, map3.size());

        SplitBrainTestSupport.unblockCommunicationBetween(instance1, instance3);
        SplitBrainTestSupport.unblockCommunicationBetween(instance2, instance3);

        assertClusterSizeEventually(3, instance1, instance2, instance3);

        assertEquals(100, map1.size());
        assertTrueEventually(() -> assertEquals(100, map3.size()));

        int size1 = map1.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size1);

        int size3 = map3.keySet(Predicates.sql("child.id > 19")).size();
        assertEquals(80, size3);
    }

}
