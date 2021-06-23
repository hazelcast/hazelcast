/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DmlPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@RunWith(JUnitParamsRunner.class)
public class JetPlanExecutorTest {

    @InjectMocks
    private JetPlanExecutor planExecutor;

    @Mock
    private MappingCatalog catalog;

    @Mock
    private HazelcastInstance hazelcastInstance;

    @Mock
    private JetService jet;

    @Mock
    private Map<String, QueryResultProducer> resultConsumerRegistry;

    @Mock
    private DAG dag;

    @Mock
    private Job job;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        given(job.getFuture()).willReturn(new CompletableFuture<>());
    }

    @Test
    @Parameters({
            "true, false",
            "false, true"
    })
    public void test_createMappingExecution(boolean replace, boolean ifNotExists) {
        // given
        Mapping mapping = mapping();
        CreateMappingPlan plan = new CreateMappingPlan(planKey(), mapping, replace, ifNotExists, planExecutor);

        // when
        SqlResult result = planExecutor.execute(plan);

        // then
        assertThat(result.updateCount()).isEqualTo(0);
        verify(catalog).createMapping(mapping, replace, ifNotExists);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_dropMappingExecution(boolean ifExists) {
        // given
        String name = "name";
        DropMappingPlan plan = new DropMappingPlan(planKey(), name, ifExists, planExecutor);

        // when
        SqlResult result = planExecutor.execute(plan);

        // then
        assertThat(result.updateCount()).isEqualTo(0);
        verify(catalog).removeMapping(name, ifExists);
    }

    @Test
    public void test_insertExecution() {
        // given
        QueryId queryId = QueryId.create(UuidUtil.newSecureUUID());
        DmlPlan plan = new DmlPlan(
                Operation.INSERT,
                planKey(),
                QueryParameterMetadata.EMPTY,
                emptySet(),
                dag,
                planExecutor
        );

        given(hazelcastInstance.getJet()).willReturn(jet);
        given(jet.newLightJob(eq(dag), isA(JobConfig.class))).willReturn(job);

        // when
        SqlResult result = planExecutor.execute(plan, queryId, emptyList());

        // then
        assertThat(result.updateCount()).isEqualTo(0);
        verify(job).join();
    }

    private static PlanKey planKey() {
        return new PlanKey(emptyList(), "");
    }

    private static Mapping mapping() {
        return new Mapping("name", "name", "type", emptyList(), emptyMap());
    }
}
