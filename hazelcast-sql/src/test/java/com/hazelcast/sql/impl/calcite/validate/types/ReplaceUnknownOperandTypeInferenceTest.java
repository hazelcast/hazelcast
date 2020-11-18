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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteralFunction;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlTypedLiteral;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastTrimFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplaceUnknownOperandTypeInferenceTest extends SqlTestSupport {

    private static final RelDataType RETURN_TYPE = type(SqlTypeName.BIGINT);

    @Test
    public void test_default() {
        ReplaceUnknownOperandTypeInference inference = new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR);

        // Empty
        RelDataType[] operandTypes = new RelDataType[] { };
        RelDataType[] expectedTypes = emptyOperandTypes(0);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);

        // Known
        operandTypes = new RelDataType[] { type(SqlTypeName.INTEGER), type(SqlTypeName.BIGINT) };
        expectedTypes = emptyOperandTypes(2);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.INTEGER, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, expectedTypes[1].getSqlTypeName());

        // Unknown
        operandTypes = new RelDataType[] { typeUnknown(), typeUnknown() };
        expectedTypes = emptyOperandTypes(2);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[1].getSqlTypeName());

        // Mixed
        operandTypes = new RelDataType[] { type(SqlTypeName.INTEGER), typeUnknown() };
        expectedTypes = emptyOperandTypes(2);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.INTEGER, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[1].getSqlTypeName());

        operandTypes = new RelDataType[] { typeUnknown(), type(SqlTypeName.BIGINT) };
        expectedTypes = emptyOperandTypes(2);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, expectedTypes[1].getSqlTypeName());
    }

    @Test
    public void test_positioned() {
        ReplaceUnknownOperandTypeInference inference = new ReplaceUnknownOperandTypeInference(
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.BIGINT },
            SqlTypeName.DECIMAL
        );

        // Empty
        RelDataType[] operandTypes = new RelDataType[] { };
        RelDataType[] expectedTypes = emptyOperandTypes(0);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);

        // No replace
        operandTypes = new RelDataType[] { type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR) };
        expectedTypes = emptyOperandTypes(3);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[1].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[2].getSqlTypeName());

        // Replace at position
        operandTypes = new RelDataType[] { typeUnknown(), type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR) };
        expectedTypes = emptyOperandTypes(3);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.INTEGER, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[1].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[2].getSqlTypeName());

        operandTypes = new RelDataType[] { type(SqlTypeName.VARCHAR), typeUnknown(), type(SqlTypeName.VARCHAR) };
        expectedTypes = emptyOperandTypes(3);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, expectedTypes[1].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[2].getSqlTypeName());

        operandTypes = new RelDataType[] { type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR), typeUnknown() };
        expectedTypes = emptyOperandTypes(3);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, expectedTypes[1].getSqlTypeName());
        assertEquals(SqlTypeName.DECIMAL, expectedTypes[2].getSqlTypeName());

        operandTypes = new RelDataType[] { typeUnknown(), typeUnknown(), typeUnknown() };
        expectedTypes = emptyOperandTypes(3);
        inference.inferOperandTypes(binding(operandTypes), RETURN_TYPE, expectedTypes);
        assertEquals(SqlTypeName.INTEGER, expectedTypes[0].getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, expectedTypes[1].getSqlTypeName());
        assertEquals(SqlTypeName.DECIMAL, expectedTypes[2].getSqlTypeName());
    }

    private static RelDataType type(SqlTypeName typeName) {
        return HazelcastTypeFactory.INSTANCE.createSqlType(typeName);
    }

    private static RelDataType typeUnknown() {
        return HazelcastTypeFactory.INSTANCE.createUnknownType();
    }

    private RelDataType[] emptyOperandTypes(int count) {
        RelDataType[] res = new RelDataType[count];

        for(int i = 0; i < res.length; i++) {
            res[i] = typeUnknown();
        }

        return res;
    }

    private static SqlCallBinding binding(RelDataType... operandTypes) {
        if (operandTypes == null) {
            operandTypes = new RelDataType[0];
        }

        HazelcastSqlValidator validator = new HazelcastSqlValidator(
            new MockCatalogReader(),
            HazelcastTypeFactory.INSTANCE,
            HazelcastSqlConformance.INSTANCE
        );

        SqlNode[] operands = new SqlNode[operandTypes.length];

        for (int i = 0; i < operands.length; i++) {
            SqlBasicCall operand = new SqlBasicCall(
                HazelcastSqlLiteralFunction.INSTANCE,
                new SqlNode[] {
                    new HazelcastSqlTypedLiteral(SqlLiteral.createNull(SqlParserPos.ZERO), null, operandTypes[i].getSqlTypeName())
                },
                SqlParserPos.ZERO
            );

            operands[i] = operand;
        }

        SqlCall call = new SqlBasicCall(new HazelcastTrimFunction(), operands, SqlParserPos.ZERO);

        return new SqlCallBinding(validator, validator.getEmptyScope(), call);
    }

    private static class MockCatalogReader implements SqlValidatorCatalogReader {
        @Override
        public SqlValidatorTable getTable(List<String> names) {
            return null;
        }

        @Override
        public RelDataType getNamedType(SqlIdentifier typeName) {
            return null;
        }

        @Override
        public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
            return null;
        }

        @Override
        public List<List<String>> getSchemaPaths() {
            return null;
        }

        @Override
        public RelDataTypeField field(RelDataType rowType, String alias) {
            return null;
        }

        @Override
        public SqlNameMatcher nameMatcher() {
            return SqlNameMatchers.liberal();
        }

        @Override
        public boolean matches(String string, String name) {
            return false;
        }

        @Override
        public RelDataType createTypeFromProjection(RelDataType type, List<String> columnNameList) {
            return null;
        }

        @Override
        public boolean isCaseSensitive() {
            return false;
        }

        @Override
        public CalciteSchema getRootSchema() {
            return null;
        }

        @Override
        public CalciteConnectionConfig getConfig() {
            return null;
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }
    }
}
