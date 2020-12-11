/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.validate;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.schema.JetTableFunctionParameter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCastFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastObjectType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static java.util.Collections.singletonList;
import static org.apache.calcite.sql.SqlLiteral.createApproxNumeric;
import static org.apache.calcite.sql.SqlLiteral.createBoolean;
import static org.apache.calcite.sql.SqlLiteral.createExactNumeric;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

@RunWith(JUnitParamsRunner.class)
public class JetSqlOperatorTableTest {

    private static final RelDataTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;

    @Mock
    private TableFunction tableFunction;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @SuppressWarnings({"unused", "checkstyle:LineLength"})
    private Object[] invalidNodes() {
        return new Object[]{
                new Object[]{new SqlBasicCall(
                        new SqlArrayValueConstructor(),
                        new SqlNode[]{literal("value")},
                        ZERO
                )},
                new Object[]{new SqlBasicCall(
                        new HazelcastSqlCastFunction(),
                        new SqlNode[]{literal("true"), new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BOOLEAN, ZERO), ZERO)},
                        ZERO
                )},
                new Object[]{createBoolean(true, ZERO)},
                new Object[]{createExactNumeric("9223372036854775", ZERO)},
                new Object[]{createApproxNumeric("9223372036854775.123", ZERO)}
        };
    }

    @Test
    @Parameters(method = "invalidNodes")
    public void when_getRowTypeWithInvalidNode_then_throws(SqlNode node) {
        SqlUserDefinedTableFunction sqlFunction = function("invalid", SqlTypeName.VARCHAR);

        assertThatThrownBy(() -> sqlFunction.getRowType(TYPE_FACTORY, singletonList(node)))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Actual argument");
    }

    @SuppressWarnings({"unused", "checkstyle:LineLength"})
    private Object[] validNodes() {
        return new Object[]{
                new Object[]{
                        new SqlBasicCall(new SqlMapValueConstructor(), new SqlNode[]{literal("key"), literal("value")}, ZERO),
                        SqlTypeName.MAP,
                        ImmutableMap.of("key", "value")
                },
                new Object[]{
                        new SqlBasicCall(SqlStdOperatorTable.DEFAULT, new SqlNode[0], ZERO),
                        SqlTypeName.VARCHAR,
                        null
                },
                new Object[]{SqlLiteral.createNull(ZERO), SqlTypeName.VARCHAR, null},
                new Object[]{SqlLiteral.createCharString("string", ZERO), SqlTypeName.VARCHAR, "string"},
        };
    }

    @Test
    @Parameters(method = "validNodes")
    public void when_getRowTypeWithValidNode_then_returnsValue(SqlNode node, SqlTypeName type, Object expected) {
        SqlUserDefinedTableFunction sqlFunction = function("valid", type);
        given(tableFunction.getRowType(TYPE_FACTORY, singletonList(expected))).willReturn(HazelcastObjectType.INSTANCE);

        RelDataType rowType = sqlFunction.getRowType(TYPE_FACTORY, singletonList(node));

        assertThat(rowType).isEqualTo(HazelcastObjectType.INSTANCE);
    }

    @SuppressWarnings("unused")
    private Object[] invalidMapNodes() {
        return new Object[]{
                new Object[]{new SqlBasicCall(
                        new SqlMapValueConstructor(),
                        new SqlNode[]{createExactNumeric("1", ZERO), literal("value")},
                        ZERO
                )},
                new Object[]{new SqlBasicCall(
                        new SqlMapValueConstructor(),
                        new SqlNode[]{literal("key"), createExactNumeric("1", ZERO)},
                        ZERO
                )},
        };
    }

    @Test
    @Parameters(method = "invalidMapNodes")
    public void when_getRowTypeWithInvalidMapNode_then_throws(SqlNode node) {
        SqlUserDefinedTableFunction sqlFunction = function("invalidMap", SqlTypeName.MAP);

        assertThatThrownBy(() -> sqlFunction.getRowType(TYPE_FACTORY, singletonList(node)))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("All values in the MAP constructor");
    }

    @Test
    public void when_duplicateEntryInMap_then_throws() {
        SqlUserDefinedTableFunction sqlFunction = function("duplicatedMap", SqlTypeName.MAP);

        assertThatThrownBy(() -> sqlFunction.getRowType(TYPE_FACTORY, singletonList(new SqlBasicCall(
                new SqlMapValueConstructor(),
                new SqlNode[]{literal("key"), literal("value1"), literal("key"), literal("value2")},
                ZERO
        )))).isInstanceOf(QueryException.class)
            .hasMessageContaining("Duplicate entry in the MAP constructor");
    }

    private SqlUserDefinedTableFunction function(String parameterName, SqlTypeName type) {
        FunctionParameter parameter = new JetTableFunctionParameter(0, parameterName, type, true);
        given(tableFunction.getParameters()).willReturn(singletonList(parameter));
        return JetSqlOperatorTable.from(tableFunction, "test_function");
    }

    private static SqlNode literal(String value) {
        return SqlLiteral.createCharString(value, ZERO);
    }
}
