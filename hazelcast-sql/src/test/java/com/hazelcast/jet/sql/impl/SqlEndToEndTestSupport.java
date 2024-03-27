/*
 * Copyright 2024 Hazelcast Inc.
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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.JobInvocationObserver;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.PartitioningStrategyUtil.constructAttributeBasedKey;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class SqlEndToEndTestSupport extends SqlTestSupport {
    protected ExpressionEvalContext eec;

    protected NodeEngineImpl nodeEngine;
    protected SqlServiceImpl sqlService;
    protected PlanExecutor planExecutor;
    protected JobCoordinationService jobCoordinationService;
    protected SqlJobInvocationObserverImpl sqlJobInvocationObserver;
    protected JobInvocationObserverImpl jobInvocationObserver;


    @Before
    public void setUp() throws Exception {
        nodeEngine = getNodeEngineImpl(instance());
        sqlJobInvocationObserver = new SqlJobInvocationObserverImpl();
        jobInvocationObserver = new JobInvocationObserverImpl();
        sqlService = (SqlServiceImpl) instance().getSql();
        planExecutor = sqlService.getOptimizer().getPlanExecutor();
        jobCoordinationService = getJetServiceBackend(instance()).getJobCoordinationService();

        planExecutor.registerJobInvocationObserver(sqlJobInvocationObserver);
        jobCoordinationService.registerInvocationObserver(jobInvocationObserver);

        eec = ExpressionEvalContext.createContext(
                emptyList(),
                instance(),
                Util.getSerializationService(instance()),
                null
        );
    }

    @After
    public void teardown() {
        jobCoordinationService.unregisterInvocationObserver(jobInvocationObserver);
    }

    SqlPlanImpl.SelectPlan assertQueryPlan(String query) {
        SqlStatement sql = new SqlStatement(query);
        SqlPlan plan = sqlService.prepare(
                sql.getSchema(),
                query,
                sql.getParameters(),
                SqlExpectedResultType.ROWS,
                NoOpSqlSecurityContext.INSTANCE);

        assertInstanceOf(SqlPlanImpl.SelectPlan.class, plan);
        return (SqlPlanImpl.SelectPlan) plan;
    }

    SqlPlanImpl.DmlPlan assertDmlQueryPlan(String query) {
        SqlStatement sql = new SqlStatement(query);
        SqlPlan plan = sqlService.prepare(
                sql.getSchema(),
                query,
                sql.getParameters(),
                SqlExpectedResultType.UPDATE_COUNT,
                NoOpSqlSecurityContext.INSTANCE);

        assertInstanceOf(SqlPlanImpl.DmlPlan.class, plan);
        return (SqlPlanImpl.DmlPlan) plan;
    }

    void assertQueryResult(SqlPlanImpl.SelectPlan selectPlan, Collection<Row> expectedResults, Object... args) {
        List<Object> arguments = Collections.emptyList();
        if (args.length > 0) {
            arguments = Arrays.asList(args);
            eec = ExpressionEvalContext.createContext(
                    arguments,
                    instance(),
                    Util.getSerializationService(instance()),
                    null
            );
        }
        QueryId queryId = QueryId.create(UUID.randomUUID());
        SqlResult result = planExecutor.execute(selectPlan,
                queryId, arguments, 0L, NoOpSqlSecurityContext.INSTANCE);
        assertCollection(expectedResults, collectResult(result));
    }

    @Nonnull
    protected static ArrayList<Row> collectResult(SqlResult result) {
        var actualRows = new ArrayList<Row>();
        for (SqlRow r : result) {
            actualRows.add(new Row(r));
        }
        return actualRows;
    }

    @Nonnull
    protected ObjectAssert<DAG> assertThatDag() {
        return Assertions.<DAG>assertThat(sqlJobInvocationObserver.dag);
    }

    protected void assertInvokedOnlyOnMembers(HazelcastInstance... members) {
        Set<Address> expectedMembers = Arrays.stream(members)
                .map(JetTestSupport::getAddress)
                .collect(Collectors.toSet());
        assertEquals(expectedMembers, jobInvocationObserver.getMembers());
    }

    protected void configureMapWithAttributes(String mapName, String... attributes) {
        MapConfig mc = new MapConfig(mapName);
        for (String attribute : attributes) {
            mc.getPartitioningAttributeConfigs()
                    .add(new PartitioningAttributeConfig(attribute));
        }
        instance().getConfig().addMapConfig(mc);
    }

    protected static class SqlJobInvocationObserverImpl implements SqlJobInvocationObserver {
        public DAG dag;
        public JobConfig jobConfig;

        @Override
        public void onJobInvocation(DAG dag, JobConfig config) {
            this.dag = dag;
            this.jobConfig = config;
        }
    }

    protected static class JobInvocationObserverImpl implements JobInvocationObserver {
        public long jobId;
        public Set<MemberInfo> members;
        public DAG dag;
        public JobConfig jobConfig;

        @Override
        public void onJobInvocation(long jobId, Map<MemberInfo, ExecutionPlan> planMap, DAG dag, JobConfig jobConfig) {
            this.jobId = jobId;
            this.members = planMap.keySet();
            this.dag = dag;
            this.jobConfig = jobConfig;
        }

        @Override
        public void onLightJobInvocation(long jobId, Set<MemberInfo> members, DAG dag, JobConfig jobConfig) {
            this.jobId = jobId;
            this.members = members;
            this.dag = dag;
            this.jobConfig = jobConfig;
        }

        public Set<Address> getMembers() {
            return members.stream().map(MemberInfo::getAddress).collect(toSet());
        }
    }

    @SuppressWarnings({"SameParameterValue", "DanglingJavadoc"})
    /**
     * Calculates expected partitions and members to participate in the query execution.
     *
     * @param shouldUseCoordinator           whether coordinator should be included in the expected members
     * @param arity                          number of fields in partitioning attribute key
     *                                       (e.g. 2 for f0, f1)
     * @param partitionedPredicateConstants  constants used in the predicate in query
     */
    static Tuple2<Set<Address>, Set<Integer>> calculateExpectedPartitions(
            NodeEngine nodeEngine,
            Map<Address, int[]> partitionAssignment,
            boolean shouldUseCoordinator,
            int arity,
            int... partitionedPredicateConstants) {
        HazelcastInstance hz = nodeEngine.getHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();
        Map<Integer, Address> reversedPartitionAssignment = new HashMap<>();
        for (Map.Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int partitionId : entry.getValue()) {
                reversedPartitionAssignment.put(partitionId, entry.getKey());
            }
        }

        Set<Integer> expectedPartitionsToParticipate = new HashSet<>();
        Set<Address> expectedMembersToParticipate = new HashSet<>();
        for (int equalityConstants : partitionedPredicateConstants) {
            Object[] constants = new Object[arity];
            Arrays.fill(constants, equalityConstants);
            Data keyData = nodeEngine.getSerializationService().toData(constructAttributeBasedKey(constants), v -> v);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
            assertTrue(reversedPartitionAssignment.containsKey(partitionId));

            expectedPartitionsToParticipate.add(partitionId);
            expectedMembersToParticipate.add(reversedPartitionAssignment.get(partitionId));
        }

        if (shouldUseCoordinator) {
            expectedMembersToParticipate.add(hz.getCluster().getLocalMember().getAddress());
            expectedPartitionsToParticipate.add(partitionService.getPartition("").getPartitionId());
        }

        return Tuple2.tuple2(expectedMembersToParticipate, expectedPartitionsToParticipate);
    }
}
