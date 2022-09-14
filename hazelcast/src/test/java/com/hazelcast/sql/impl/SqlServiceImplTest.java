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
package com.hazelcast.sql.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.SqlConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SqlServiceImplTest {

    @Mock
    private NodeEngineImpl nodeEngine;

    @Mock
    private ILogger logger;

    @Mock
    private Config config;

    @Mock
    private SqlOptimizer sqlOptimizer;

    @Mock
    private SqlPlan sqlPlan;

    @Mock
    private AbstractSqlResult sqlResult;

    private SqlServiceImpl sqlService;

    @Before
    public void setUp() throws Exception {
        when(nodeEngine.getLogger(SqlServiceImpl.class)).thenReturn(logger);
        when(nodeEngine.getConfig()).thenReturn(config);
        when(config.getSqlConfig()).thenReturn(new SqlConfig());

        sqlService = new SqlServiceImpl(nodeEngine);
        sqlService.setOptimizer(sqlOptimizer);

        MemberImpl localMember = new MemberImpl();
        when(nodeEngine.getLocalMember()).thenReturn(localMember);
        when(config.getJetConfig()).thenReturn(new JetConfig().setEnabled(true));
        when(sqlOptimizer.prepare(any())).thenReturn(sqlPlan);
    }

    @Test
    public void execute_batchQuery() {
        // Given
        QueryId queryId = new QueryId();
        when(sqlPlan.execute(eq(queryId), any(), eq(0L))).thenReturn(sqlResult);
        SqlStatement sqlStatement = new SqlStatement("SELECT * FROM map");

        // When
        SqlResult actualSqlResult = sqlService.execute(sqlStatement, NoOpSqlSecurityContext.INSTANCE, queryId, false);

        // Then
        assertThat(actualSqlResult).isEqualTo(sqlResult);
        assertThat(sqlService.getSqlQueriesSubmittedCount()).isEqualTo(1L);
        assertThat(sqlService.getSqlStreamingQueriesSubmittedCount()).isEqualTo(0L);
    }

    @Test
    public void execute_streamingQuery() {
        // Given
        QueryId queryId = new QueryId();
        when(sqlPlan.execute(eq(queryId), any(), eq(0L))).thenReturn(sqlResult);
        when(sqlResult.isInfiniteRows()).thenReturn(true);
        SqlStatement sqlStatement = new SqlStatement("SELECT * FROM TABLE(generate_stream(10))");

        // When
        SqlResult actualSqlResult = sqlService.execute(sqlStatement, NoOpSqlSecurityContext.INSTANCE, queryId, false);

        // Then
        assertThat(actualSqlResult).isEqualTo(sqlResult);
        assertThat(sqlService.getSqlQueriesSubmittedCount()).isEqualTo(1L);
        assertThat(sqlService.getSqlStreamingQueriesSubmittedCount()).isEqualTo(1L);
    }
}
