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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.SpecificPartitionsImapReaderPms.mapReader;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SpecificPartitionsImapReaderPmsTest extends SimpleTestInClusterSupport {
    private static final int ITERATIONS = 1000;
    private int pKey;
    private int keyOwnerPartitionId;
    private String mapName;
    private String sinkName;
    private JobConfig jobConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(5, null);
    }

    @Before
    public void before() throws Exception {
        mapName = randomName();
        sinkName = randomName();

        jobConfig = new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList());

        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        Address ownderAddress = instance().getCluster().getLocalMember().getAddress();
        for (int i = 1; i < ITERATIONS; ++i) {
            int pIdCandidate = instance().getPartitionService().getPartition(i).getPartitionId();
            if (Arrays.binarySearch(partitionAssignment.get(ownderAddress), pIdCandidate) >= 0) {
                pKey = i;
                keyOwnerPartitionId = pIdCandidate;
                break;
            }
        }
    }

    // Note: basic functionality must work even without member pruning used.
    @Test
    public void test_basic() {
        // Basic test is performed as submitting job by DAG.
        IMap<Integer, Integer> map = instance().getMap(mapName);
        map.put(0, 0);

        DAG dag = new DAG();
        ProcessorMetaSupplier readPms = mapReader(mapName, null, List.of(
                List.of(ConstantExpression.create(0, QueryDataType.INT))));
        Vertex source = dag.newVertex("source", readPms);
        Vertex sink = dag.newVertex("sink", writeMapP(sinkName));

        dag.edge(between(source, sink));
        instance().getJet().newLightJob(dag, jobConfig).join();

        assertEquals(0, instance().getMap(sinkName).get(0));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void test_prunable() {
        String mapName = "m";
        IMap<Integer, Integer> map = instance().getMap(mapName);
        map.put(pKey, pKey);

        // Given
        ProcessorMetaSupplier pms = mapReader(mapName, null, List.of(
                List.of(ConstantExpression.create(pKey, QueryDataType.INT))
        ));

        // When
        // Note: processor verification support is bounded to one member only,
        //  so it is applicable to make this test in 'processor verification' way.
        TestSupport.verifyProcessor(adaptSupplier(pms))
                .hazelcastInstance(instance())
                .jobConfig(jobConfig)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(entry(pKey, pKey)));

        // Then
        Assert.assertEquals(1, ((SpecificPartitionsImapReaderPms) pms).partitionsToScan.length);
        Assert.assertEquals(keyOwnerPartitionId, ((SpecificPartitionsImapReaderPms) pms).partitionsToScan[0]);
    }
}
