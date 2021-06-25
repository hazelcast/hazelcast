/*
 * Copyright 2021 Hazelcast Inc.
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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.parse.SqlCreateMapping;
import com.hazelcast.jet.sql.impl.parse.SqlDataType;
import com.hazelcast.jet.sql.impl.parse.SqlDropMapping;
import com.hazelcast.jet.sql.impl.parse.SqlMappingColumn;
import com.hazelcast.jet.sql.impl.parse.SqlOption;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class JetSqlBackendTest {

    @InjectMocks
    private JetSqlBackend sqlBackend;

    @Mock
    private NodeEngine nodeEngine;

    @Mock
    private JetPlanExecutor planExecutor;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @Parameters({
            "true, false",
            "false, true"
    })
    public void test_createMappingPlan(boolean replace, boolean ifNotExists) {
        // given
        SqlCreateMapping node = new SqlCreateMapping(
                identifier("mapping_name"),
                identifier("external_mapping_name"),
                nodeList(column("column_name", INT, "external_column_name")),
                identifier("mapping_type"),
                nodeList(option("option_key", "option_value")),
                replace,
                ifNotExists,
                SqlParserPos.ZERO
        );
        QueryParseResult parseResult = new QueryParseResult(node, null, null, null, false);

        // when
        CreateMappingPlan plan = (CreateMappingPlan) sqlBackend.createPlan(task(), parseResult, null);

        // then
        assertThat(plan.mapping().name()).isEqualTo("mapping_name");
        assertThat(plan.mapping().externalName()).isEqualTo("external_mapping_name");
        assertThat(plan.mapping().type()).isEqualTo("mapping_type");
        assertThat(plan.mapping().fields())
                .isEqualTo(singletonList(new MappingField("column_name", INT, "external_column_name")));
        assertThat(plan.mapping().options()).isEqualTo(ImmutableMap.of("option_key", "option_value"));
        assertThat(plan.replace()).isEqualTo(replace);
        assertThat(plan.ifNotExists()).isEqualTo(ifNotExists);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_removeMappingPlan(boolean ifExists) {
        // given
        SqlDropMapping node = new SqlDropMapping(
                identifier("mapping_name"),
                ifExists,
                SqlParserPos.ZERO
        );
        QueryParseResult parseResult = new QueryParseResult(node, null, null, null, false);

        // when
        DropMappingPlan plan = (DropMappingPlan) sqlBackend.createPlan(task(), parseResult, null);

        // then
        assertThat(plan.name()).isEqualTo("mapping_name");
        assertThat(plan.ifExists()).isEqualTo(ifExists);
    }

    private static SqlNodeList nodeList(SqlNode... nodes) {
        return new SqlNodeList(asList(nodes), SqlParserPos.ZERO);
    }

    private static SqlMappingColumn column(String name, QueryDataType type, String externalName) {
        return new SqlMappingColumn(
                identifier(name),
                new SqlDataType(type, SqlParserPos.ZERO),
                identifier(externalName),
                SqlParserPos.ZERO
        );
    }

    private static OptimizationTask task() {
        return new OptimizationTask("", emptyList(), emptyList(), null);
    }

    private static SqlOption option(String key, String value) {
        return new SqlOption(
                SqlLiteral.createCharString(key, SqlParserPos.ZERO),
                SqlLiteral.createCharString(value, SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }

    private static SqlIdentifier identifier(String name) {
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }
}
