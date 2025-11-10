/*
 * Copyright 2025 Hazelcast Inc.
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
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.misc.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.SqlTestSupport.createMapping;
import static com.hazelcast.jet.sql.impl.SqlEndToEndTestSupport.calculateExpectedPartitions;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPrunedMemberLeftTest extends JetTestSupport {
    // Page size is 4096, so we'll fetch 2 pages.
    private static final int MAP_ENTRIES = (int) (SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE * 1.5);
    private static final int CLUSTER_SIZE = 5;
    private static final int CONSTANT = 2;
    private static final String MAP_NAME = "map";

    private HazelcastInstance[] hz;
    private NodeEngine masterNodeEngine;
    private Map<Address, HazelcastInstance> addrToInstanceMap;

    @Parameterized.Parameters(name = "analyze={0}, graceful={1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (boolean analyze : asList(false, true)) {
            for (boolean graceful : asList(false, true)) {
                params.add(new Object[] { analyze, graceful });
            }
        }

        return params;
    }

    @Parameterized.Parameter
    public boolean analyze;

    @Parameterized.Parameter(1)
    public boolean graceful;


    @Before
    public void setUp() throws Exception {
        hz = new HazelcastInstance[CLUSTER_SIZE];
        addrToInstanceMap = new LinkedHashMap<>();
        for (int i = 0; i < CLUSTER_SIZE; ++i) {
            hz[i] = createHazelcastInstance(regularInstanceConfig());
            addrToInstanceMap.put(hz[i].getCluster().getLocalMember().getAddress(), hz[i]);
        }
        masterNodeEngine = Accessors.getNodeEngineImpl(hz[0]);

        prepareMap();
        createMapping(hz[0], MAP_NAME, Pojo.class, Integer.class);
    }

    @Test
    // https://hazelcast.atlassian.net/browse/HZ-2992
    public void test_queryDoesNotTerminateWhenPrunedMemberLeavesCluster() {
        // Given
        String query = (analyze ? "ANALYZE " : "") + "SELECT * FROM " + MAP_NAME + " WHERE f0 = " + CONSTANT;
        HazelcastInstance prunedInstanceToTerminate = findPrunedAndNonPrunedInstance().f0();
        HazelcastInstance nonPrunedInstance = findPrunedAndNonPrunedInstance().f1();
        int cnt = 0;

        for (SqlRow row : nonPrunedInstance.getSql().execute(query)) {
            // Shutdown chosen member after we read 1st entry.
            if (cnt == 0) {
                // When
                if (graceful) {
                    prunedInstanceToTerminate.shutdown();
                } else {
                    prunedInstanceToTerminate.getLifecycleService().terminate();
                }
                assertClusterSizeEventually(CLUSTER_SIZE - 1,
                        Arrays.stream(hz).filter(i -> i != prunedInstanceToTerminate).collect(Collectors.toList()));

                // Then
                assertThat(nonPrunedInstance.getJet().getJobs())
                        .as("The query should be still running")
                        .anyMatch(j -> j.getStatus() == JobStatus.RUNNING);
            }
            ++cnt;
        }
        // Then
        assertEquals(MAP_ENTRIES, cnt);
    }

    private Tuple2<HazelcastInstance, HazelcastInstance> findPrunedAndNonPrunedInstance() {
        Tuple2<Set<Address>, Set<Integer>> expectedPartitionsAndParticipants =
                calculateExpectedPartitions(masterNodeEngine, getPartitionAssignment(hz[0]), true, 1, CONSTANT);

        Set<Address> addresses = new HashSet<>(addrToInstanceMap.keySet());
        assertNotNull(expectedPartitionsAndParticipants.f0());
        assertTrue(addresses.removeAll(expectedPartitionsAndParticipants.f0()));
        Address toRemove = addresses.iterator().next();
        return Tuple2.tuple2(
                addrToInstanceMap.get(toRemove),
                addrToInstanceMap.get(expectedPartitionsAndParticipants.f0().iterator().next())
        );
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

