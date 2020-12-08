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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplaceUnknownOperandTypeInferenceTest extends InferTypesTestSupport {

    private static final SqlCallBinding BINDING = createBinding();
    private static final RelDataType RETURN_TYPE = type(SqlTypeName.BIGINT);

    @Test
    public void test_default() {
        ReplaceUnknownOperandTypeInference inference = new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR);

        // Empty
        RelDataType[] operandTypes = new RelDataType[] { };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);

        // Known
        operandTypes = new RelDataType[] { type(SqlTypeName.INTEGER), type(SqlTypeName.BIGINT) };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.INTEGER), operandTypes[0]);
        assertEquals(type(SqlTypeName.BIGINT), operandTypes[1]);

        // Unknown
        operandTypes = new RelDataType[] { typeUnknown(), typeUnknown() };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[0]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[1]);

        // Mixed
        operandTypes = new RelDataType[] { type(SqlTypeName.INTEGER), typeUnknown() };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.INTEGER), operandTypes[0]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[1]);

        operandTypes = new RelDataType[] { typeUnknown(), type(SqlTypeName.BIGINT) };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[0]);
        assertEquals(type(SqlTypeName.BIGINT), operandTypes[1]);
    }

    @Test
    public void test_positioned() {
        ReplaceUnknownOperandTypeInference inference = new ReplaceUnknownOperandTypeInference(
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.BIGINT },
            SqlTypeName.DECIMAL
        );

        // Empty
        RelDataType[] operandTypes = new RelDataType[] { };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);

        // No replace
        operandTypes = new RelDataType[] { type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR) };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[0]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[1]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[2]);

        // Replace at position
        operandTypes = new RelDataType[] { typeUnknown(), type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR) };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.INTEGER), operandTypes[0]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[1]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[2]);

        operandTypes = new RelDataType[] { type(SqlTypeName.VARCHAR), typeUnknown(), type(SqlTypeName.VARCHAR) };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[0]);
        assertEquals(type(SqlTypeName.BIGINT), operandTypes[1]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[2]);

        operandTypes = new RelDataType[] { type(SqlTypeName.VARCHAR), type(SqlTypeName.VARCHAR), typeUnknown() };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[0]);
        assertEquals(type(SqlTypeName.VARCHAR), operandTypes[1]);
        assertEquals(type(SqlTypeName.DECIMAL), operandTypes[2]);

        operandTypes = new RelDataType[] { typeUnknown(), typeUnknown(), typeUnknown() };
        inference.inferOperandTypes(BINDING, RETURN_TYPE, operandTypes);
        assertEquals(type(SqlTypeName.INTEGER), operandTypes[0]);
        assertEquals(type(SqlTypeName.BIGINT), operandTypes[1]);
        assertEquals(type(SqlTypeName.DECIMAL), operandTypes[2]);
    }
}
