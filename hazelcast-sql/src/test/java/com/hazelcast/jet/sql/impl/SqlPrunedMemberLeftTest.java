/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.misc.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.sql.SqlTestSupport.createMapping;
import static com.hazelcast.jet.sql.impl.SqlEndToEndTestSupport.calculateExpectedPartitions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPrunedMemberLeftTest extends JetTestSupport {
    private static final int MAP_ENTRIES = 1_000;
    private static final String MAP_NAME = "map";

    private NodeEngine nodeEngine1;

    private HazelcastInstance hz1;
    private HazelcastInstance hz2;
    private HazelcastInstance hz3;
    private HazelcastInstance hz4;
    private HazelcastInstance hz5;

    private Map<Address, HazelcastInstance> addrToInstanceMap;

    @Before
    public void setUp() throws Exception {
        hz1 = createHazelcastInstance(regularInstanceConfig());
        nodeEngine1 = getNodeEngineImpl(hz1);
        hz2 = createHazelcastInstance(regularInstanceConfig());
        hz3 = createHazelcastInstance(regularInstanceConfig());
        hz4 = createHazelcastInstance(regularInstanceConfig());
        hz5 = createHazelcastInstance(regularInstanceConfig());

        addrToInstanceMap = new HashMap<>();
        addrToInstanceMap.put(hz1.getCluster().getLocalMember().getAddress(), hz1);
        addrToInstanceMap.put(hz2.getCluster().getLocalMember().getAddress(), hz2);
        addrToInstanceMap.put(hz3.getCluster().getLocalMember().getAddress(), hz3);
        addrToInstanceMap.put(hz4.getCluster().getLocalMember().getAddress(), hz4);
        addrToInstanceMap.put(hz5.getCluster().getLocalMember().getAddress(), hz5);

        prepareMap();
        createMapping(hz5, MAP_NAME, Pojo.class, Integer.class);
    }

    @Test
    // https://hazelcast.atlassian.net/browse/HZ-2992
    public void test() {
        // Given
        final int c = 2; // constant, assuming hz3 will be coordinator.

        Set<Integer> expected = IntStream.range(c * 10, (c + 1) * 10).boxed().collect(Collectors.toSet());
        String query = "SELECT this FROM " + MAP_NAME + " WHERE f0 = " + c;

        var instances = findPrunedAndNonPrunedInstances(c);
        HazelcastInstance prunedInstanceToTerminate = instances.f0();
        HazelcastInstance chosenCoordinator = instances.f1();

        int cnt = 0;

        for (SqlRow row : chosenCoordinator.getSql().execute(query)) {
            // Read one entry and shutdown chosen member.
            if (cnt == 1) {
                prunedInstanceToTerminate.shutdown();
            }
            Integer value = row.getObject(0);
            assertThat(expected).contains(value);
            expected.remove(value);

            ++cnt;
        }
        // Since we're filling map at {i/10, i, i} -> {i} way, we're expecting 10 entries.
        assertTrue(expected.isEmpty());
        assertEquals(10, cnt);
    }

    private Tuple2<HazelcastInstance, HazelcastInstance> findPrunedAndNonPrunedInstances(int constant) {
        Tuple2<Set<Address>, Set<Integer>> expectedPartitionsAndParticipants =
                calculateExpectedPartitions(nodeEngine1, getPartitionAssignment(hz1), false, 1, constant);

        assertNotNull(expectedPartitionsAndParticipants.f0());
        assertEquals(1, expectedPartitionsAndParticipants.f0().size());

        Set<Address> addresses = new HashSet<>(addrToInstanceMap.keySet());
        assertTrue(addresses.removeAll(expectedPartitionsAndParticipants.f0()));
        assertEquals(4, addresses.size());

        var prunedInstance = addrToInstanceMap.get(addresses.iterator().next());
        var nonPrunedInstance = addrToInstanceMap.get(expectedPartitionsAndParticipants.f0().iterator().next());
        assertNotSame(prunedInstance, nonPrunedInstance);
        return Tuple2.tuple2(prunedInstance, nonPrunedInstance);
    }

    private void prepareMap() {
        hz1.getConfig().addMapConfig(
                new MapConfig(MAP_NAME)
                        .setPartitioningAttributeConfigs(
                                Collections.singletonList(new PartitioningAttributeConfig("f0"))));

        IMap<Pojo, Integer> map = hz1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_ENTRIES; ++i) {
            map.put(new Pojo(i / 10, i, i), i);
        }
    }
}

