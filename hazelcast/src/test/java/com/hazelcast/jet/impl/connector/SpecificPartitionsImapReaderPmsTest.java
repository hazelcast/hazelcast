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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.impl.connector.SpecificPartitionsImapReaderPms.mapReader;
import static java.util.stream.Collectors.toMap;

@Category(QuickTest.class)
public class SpecificPartitionsImapReaderPmsTest extends SimpleTestInClusterSupport {
    private static final int ITERATIONS = 1000;
    private int pKey;
    private int keyOwnerPartitionId;
    private Address ownderAddress;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance());
        Map<Address, int[]> partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(nodeEngine,
                        Util.getMembersView(nodeEngine).getMembers(), null)
                .entrySet().stream().collect(toMap(en -> en.getKey().getAddress(), Map.Entry::getValue));
        ownderAddress = instance().getCluster().getLocalMember().getAddress();
        for (int i = 1; i < ITERATIONS; ++i) {
            int pIdCandidate = instance().getPartitionService().getPartition(i).getPartitionId();
            if (Arrays.binarySearch(partitionAssignment.get(ownderAddress), pIdCandidate) >= 0) {
                pKey = i;
                keyOwnerPartitionId = pIdCandidate;
                break;
            }
        }
    }


    @Test
    public void test_basic() {
        String mapName = "m";
        IMap<Integer, Integer> map = instance().getMap(mapName);
        map.put(0, 0);

        // Given
        ProcessorMetaSupplier pms = mapReader(mapName);

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, Arrays.asList(0, pKey, 2));

        // When & Then
        TestSupport.verifyProcessor(adaptSupplier(pms))
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(entry(0, 0)));

    }
}
