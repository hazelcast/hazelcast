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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.jet.JobInvocationObserver;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.misc.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.ExpressionEvalContextImpl;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPartitionPruningE2ETest extends SqlTestSupport {
    private static ExpressionEvalContext EEC;

    private SqlServiceImpl sqlService;
    private PlanExecutor planExecutor;
    private String mapName;

    private PreJobInvocationObserverImpl preJobInvocationObserver;
    private JobInvocationObserverImpl jobInvocationObserver;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
        EEC = new ExpressionEvalContextImpl(
                emptyList(),
                Util.getSerializationService(instance()),
                Util.getNodeEngine(instance()));
    }

    @Before
    public void before() throws Exception {
        sqlService = (SqlServiceImpl) instance().getSql();
        planExecutor = sqlService.getOptimizer().getPlanExecutor();

        preJobInvocationObserver = new PreJobInvocationObserverImpl();
        jobInvocationObserver = new JobInvocationObserverImpl();

        planExecutor.registerJobInvocationObserver(preJobInvocationObserver);

        JobCoordinationService jobCoordinationService = ((JetServiceBackend) getNodeEngineImpl(instance())
                .getService(JetServiceBackend.SERVICE_NAME))
                .getJobCoordinationService();
        jobCoordinationService.registerInvocationObserver(jobInvocationObserver);

        mapName = randomName();
    }

    @Test
    public void when_scanWithSimplePruningKey_then_prunable() {
        final String query = "SELECT * FROM " + mapName + " WHERE f0 = 2";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0")
                )));
        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);
        map.put(new Pojo(2, 2, 2), "2");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);
        assertQueryResult(selectPlan, singletonList(new Row(2, 2, 2, "2")));

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, EEC);
        Set<Integer> expectedPartitionsToUse = preJobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);

        assertEquals(1, partitionsToUse.size());
        assertRequiredPartitions(expectedPartitionsToUse, partitionsToUse);
    }

    @Test
    public void when_scanWithoutDefinedStrategy_then_nonPrunable() {
        final String query = "SELECT * FROM " + mapName + " WHERE f0 = 2";

        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);

        map.put(new Pojo(2, 2, 2), "2");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, EEC);

        assertEquals(0, partitionsToUse.size());
        assertQueryResult(selectPlan, singletonList(new Row(2, 2, 2, "2")));
    }

    @Test
    public void when_scanWithCompoundPruningKey_then_prunable() {
        final String query = "SELECT * FROM " + mapName + " WHERE f0 = 2 AND f1 = 2";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0"),
                        new PartitioningAttributeConfig("f1")
                )));
        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);

        map.put(new Pojo(2, 2, 2), "2");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, singletonList(new Row(2, 2, 2, "2")));

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, EEC);
        Set<Integer> expectedPartitionsToUse = preJobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);

        assertEquals(1, partitionsToUse.size());
        assertRequiredPartitions(expectedPartitionsToUse, partitionsToUse);
    }

    @Test
    public void when_selfUnionAllForOrPredicateAndSimplePruningKey_then_prunable() {
        final String query = "(SELECT * FROM " + mapName + " WHERE f0 = 2)"
                + " UNION ALL "
                + "(SELECT * FROM " + mapName + " WHERE f0 = 3)";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0")
                )));

        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);

        map.put(new Pojo(2, 2, 2), "2");
        map.put(new Pojo(3, 3, 3), "3");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, asList(new Row(2, 2, 2, "2"), new Row(3, 3, 3, "3")));

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, EEC);
        Set<Integer> expectedPartitionsToUse = preJobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);

        assertEquals(2, partitionsToUse.size());
        assertRequiredPartitions(expectedPartitionsToUse, partitionsToUse);
    }

    @Test
    public void when_selfUnionAllForOrPredicateAndCompoundPruningKey_then_prunable() {
        final String query = "(SELECT * FROM " + mapName + " WHERE f0 = 2 AND f1 = 2)"
                + " UNION ALL "
                + "(SELECT * FROM " + mapName + " WHERE f0 = 3 AND f1 = 3)";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0"),
                        new PartitioningAttributeConfig("f1")
                )));
        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);

        map.put(new Pojo(2, 2, 2), "2");
        map.put(new Pojo(3, 3, 3), "3");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, asList(new Row(2, 2, 2, "2"), new Row(3, 3, 3, "3")));

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, EEC);
        Set<Integer> expectedPartitionsToUse = preJobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);

        assertEquals(2, partitionsToUse.size());
        assertRequiredPartitions(expectedPartitionsToUse, partitionsToUse);
    }

    @Test
    public void when_unionAllTwoMapsWithCompoundPruningKey_then_prunable() {
        final String secondMapName = randomName();
        final String query = "(SELECT f2 FROM " + mapName + " WHERE f0 = 2 AND f1 = 2)"
                + " UNION ALL "
                + "(SELECT f2 FROM " + secondMapName + " WHERE f0 = 3 AND f1 = 3)";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0"),
                        new PartitioningAttributeConfig("f1")
                )));
        instance().getConfig().addMapConfig(
                new MapConfig(secondMapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0"),
                        new PartitioningAttributeConfig("f1")
                )));

        IMap<Pojo, String> map1 = instance().getMap(mapName);
        IMap<Pojo, String> map2 = instance().getMap(secondMapName);

        createMapping(mapName, Pojo.class, String.class);
        createMapping(secondMapName, Pojo.class, String.class);

        map1.put(new Pojo(2, 2, 2), "2");
        map2.put(new Pojo(3, 3, 3), "3");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, asList(new Row(2), new Row(3)));

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, EEC);
        Set<Integer> expectedPartitionsToUse = preJobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);

        assertEquals(2, partitionsToUse.size());
        assertRequiredPartitions(expectedPartitionsToUse, partitionsToUse);
    }

    @Test
    public void when_unionAllTwoMapsAndOneMapIsNotPrunable_then_nonPrunable() {
        final String secondMapName = randomName();
        final String query = "(SELECT f2 FROM " + mapName + " WHERE f0 = 2 AND f1 = 2)"
                + " UNION ALL "
                + "(SELECT f2 FROM " + secondMapName + " WHERE f0 = 3 AND f1 = 3)";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0"),
                        new PartitioningAttributeConfig("f1")
                )));

        IMap<Pojo, String> map1 = instance().getMap(mapName);
        IMap<Pojo, String> map2 = instance().getMap(secondMapName);

        createMapping(mapName, Pojo.class, String.class);
        createMapping(secondMapName, Pojo.class, String.class);

        map1.put(new Pojo(2, 2, 2), "2");
        map2.put(new Pojo(3, 3, 3), "3");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, asList(new Row(2), new Row(3)));
        assertEquals(0, planExecutor.tryUsePrunability(selectPlan, EEC).size());
    }

    @Ignore("https://hazelcast.atlassian.net/browse/HZ-2796")
    @Test
    public void when_unionForWithSimplePruningKey_then_non_prunable() {
        // Note: it is a test for the future implementation of prunable Aggregation.
        //  Union converts to UnionAll + Aggregation, and  prunable Aggregation implementor
        //  easily may miss that fact during testing.
        final String mapName = randomName();
        final String query = "(SELECT * FROM " + mapName + " WHERE f0 = 2)"
                + " UNION "
                + "(SELECT * FROM " + mapName + " WHERE f0 = 2)";

        instance().getConfig().addMapConfig(
                new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("f0")
                )));

        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);

        map.put(new Pojo(2, 2, 2), "2");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, singletonList(new Row(2, 2, 2, "2")));
        assertEquals(0, planExecutor.tryUsePrunability(selectPlan, EEC).size());
    }

    @Nonnull
    private static ArrayList<Row> collectResult(SqlResult result) {
        var actualRows = new ArrayList<Row>();
        for (SqlRow r : result) {
            actualRows.add(new Row(r));
        }
        return actualRows;
    }

    static void assertRequiredPartitions(Set<Integer> expectedPartitions, Set<Integer> actualPartitions) {
        assertNotNull(expectedPartitions);
        assertNotNull(actualPartitions);
        assertContainsAll(expectedPartitions, actualPartitions);
    }

    void assertQueryResult(SqlPlanImpl.SelectPlan selectPlan, Collection<Row> expectedResults) {
        QueryId queryId = QueryId.create(UUID.randomUUID());
        SqlResult result = planExecutor.execute(selectPlan, queryId, Collections.emptyList(), 0L);
        assertCollection(expectedResults, collectResult(result));
    }

    SqlPlanImpl.SelectPlan assertQueryPlan(String query) {
        SqlStatement sql = new SqlStatement(query);
        SqlPlan plan = sqlService.prepare(
                sql.getSchema(),
                query,
                sql.getParameters(),
                SqlExpectedResultType.ROWS
        );

        assertInstanceOf(SqlPlanImpl.SelectPlan.class, plan);
        return (SqlPlanImpl.SelectPlan) plan;
    }

    Tuple2<Set<Address>, Set<Integer>> calculateExpectedPartitions(
            boolean shouldUseCoordinator, int... equalityConstantsInPredicate) {
        IPartitionService partitionService = getNodeEngineImpl(instance()).getPartitionService();
        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        Map<Integer, Address> reversedPartitionAssignment = new HashMap<>();
        for (Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int partitionId : entry.getValue()) {
                reversedPartitionAssignment.put(partitionId, entry.getKey());
            }
        }

        Set<Integer> expectedPartitionsToParticipate = new HashSet<>();
        Set<Address> expectedMembersToParticipate = new HashSet<>();
        for (int equalityConstant : equalityConstantsInPredicate) {
            int partitionId = partitionService.getPartitionId(equalityConstant);
            assertTrue(reversedPartitionAssignment.containsKey(partitionId));
            expectedPartitionsToParticipate.add(partitionId);
            expectedMembersToParticipate.add(reversedPartitionAssignment.get(partitionId));
        }

        if (shouldUseCoordinator) {
            expectedMembersToParticipate.add(instance().getCluster().getLocalMember().getAddress());
            expectedPartitionsToParticipate.add(partitionService.getPartitionId(""));
        }

        return Tuple2.tuple2(expectedMembersToParticipate, expectedPartitionsToParticipate);
    }

    static class PreJobInvocationObserverImpl implements PreJobInvocationObserver {
        public DAG dag;
        public JobConfig jobConfig;

        @Override
        public void onJobInvocation(DAG dag, JobConfig config) {
            this.dag = dag;
            this.jobConfig = config;
        }
    }

    static class JobInvocationObserverImpl implements JobInvocationObserver {
        public long jobId;
        public List<MemberInfo> members;
        public DAG dag;
        public JobConfig jobConfig;

        @Override
        public void onJobInvocation(long jobId, List<MemberInfo> members, DAG dag, JobConfig jobConfig) {
            this.jobId = jobId;
            this.members = members;
            this.dag = dag;
            this.jobConfig = jobConfig;
        }
    }
}
