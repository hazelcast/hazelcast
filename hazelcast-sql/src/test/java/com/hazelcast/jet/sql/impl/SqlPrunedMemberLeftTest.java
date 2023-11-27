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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.sql.SqlTestSupport.createMapping;
import static com.hazelcast.jet.sql.impl.SqlEndToEndTestSupport.calculateExpectedPartitions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPrunedMemberLeftTest extends JetTestSupport {
    // Page size is 4096, so we'll fetch 2 pages.
    private static final int MAP_ENTRIES = 5_000;
    private static final int CLUSTER_SIZE = 5;
    private static final int CONSTANT = 2;
    private static final String MAP_NAME = "map";

    private HazelcastInstance hz[];
    private HazelcastInstance client;
    private NodeEngine masterNodeEngine;
    private Map<Address, HazelcastInstance> addrToInstanceMap;

    @Before
    public void setUp() throws Exception {
        hz = new HazelcastInstance[CLUSTER_SIZE];
        addrToInstanceMap = new LinkedHashMap<>();
        for (int i = 0; i < CLUSTER_SIZE; ++i) {
            hz[i] = createHazelcastInstance(regularInstanceConfig());
            addrToInstanceMap.put(hz[i].getCluster().getLocalMember().getAddress(), hz[i]);
        }
        client = createHazelcastClient();
        masterNodeEngine = getNodeEngineImpl(hz[0]);

        prepareMap();
        createMapping(client, MAP_NAME, Pojo.class, Integer.class);
    }

    @Test
    // https://hazelcast.atlassian.net/browse/HZ-2992
    public void test() {
        // Given
        String query = "SELECT this FROM " + MAP_NAME + " WHERE f0 = " + CONSTANT;

        HazelcastInstance prunedInstanceToTerminate = findPrunedInstance();

        int cnt = 0;

        for (SqlRow row : client.getSql().execute(query)) {
            // Shutdown chosen member before we read anything.
            if (cnt == 0) {
                prunedInstanceToTerminate.shutdown();
            }
            ++cnt;
        }
        assertEquals(MAP_ENTRIES, cnt);
    }

    private HazelcastInstance findPrunedInstance() {
        Tuple2<Set<Address>, Set<Integer>> expectedPartitionsAndParticipants =
                calculateExpectedPartitions(masterNodeEngine, getPartitionAssignment(hz[0]), false, 1, CONSTANT);

        assertNotNull(expectedPartitionsAndParticipants.f0());
        assertEquals(1, expectedPartitionsAndParticipants.f0().size());

        Set<Address> addresses = new HashSet<>(addrToInstanceMap.keySet());
        assertTrue(addresses.removeAll(expectedPartitionsAndParticipants.f0()));
        assertEquals(CLUSTER_SIZE - 1, addresses.size());

        return addrToInstanceMap.get(addresses.iterator().next());
    }

    private void prepareMap() {
        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setPartitioningAttributeConfigs(
                        Collections.singletonList(new PartitioningAttributeConfig("f0")));

        hz[0].getConfig().addMapConfig(mapConfig);

        IMap<Pojo, Integer> map = hz[0].getMap(MAP_NAME);
        for (int i = 0; i < MAP_ENTRIES; ++i) {
            map.put(new Pojo(CONSTANT, i, i), i);
        }
    }
}

