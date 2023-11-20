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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.JobInvocationObserver;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
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
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;

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
        jobCoordinationService = ((JetServiceBackend) nodeEngine
                .getService(JetServiceBackend.SERVICE_NAME))
                .getJobCoordinationService();

        planExecutor.registerJobInvocationObserver(sqlJobInvocationObserver);
        jobCoordinationService.registerInvocationObserver(jobInvocationObserver);

        eec = ExpressionEvalContext.createContext(
                emptyList(),
                instance(),
                Util.getSerializationService(instance()),
                null
        );
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
}
