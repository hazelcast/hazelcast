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

import com.hazelcast.sql.impl.expression.ExpressionTestBase;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes.FIRST_KNOWN;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.bitWidthOf;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastInferTypesTest {

    @Test
    public void testFirstKnown() {
        assertInference("1 = 1", FIRST_KNOWN, HazelcastIntegerType.of(1, false), HazelcastIntegerType.of(1, false));

        assertInference("null = 10", FIRST_KNOWN, HazelcastIntegerType.of(bitWidthOf(10), true),
                HazelcastIntegerType.of(bitWidthOf(10), true));
        assertInference("10 = null", FIRST_KNOWN, HazelcastIntegerType.of(bitWidthOf(10), false),
                HazelcastIntegerType.of(bitWidthOf(10), false));
        assertInference("null = null", FIRST_KNOWN, null, null);

        assertInference("? = 1", FIRST_KNOWN, HazelcastIntegerType.of(SqlTypeName.BIGINT, false),
                HazelcastIntegerType.of(SqlTypeName.BIGINT, false));
        assertInference("1 = ?", FIRST_KNOWN, HazelcastIntegerType.of(SqlTypeName.BIGINT, false),
                HazelcastIntegerType.of(SqlTypeName.BIGINT, false));
    }

    @SuppressWarnings("SameParameterValue")
    private void assertInference(String expression, SqlOperandTypeInference inference, RelDataType... expected) {
        SqlCallBinding binding = ExpressionTestBase.makeBinding(expression);
        RelDataType unknown = binding.getValidator().getUnknownType();

        RelDataType[] actual = new RelDataType[expected.length];
        Arrays.fill(actual, unknown);

        inference.inferOperandTypes(binding, unknown, actual);

        for (int i = 0; i < expected.length; ++i) {
            if (expected[i] == null) {
                expected[i] = unknown;
            }
        }
        Assert.assertArrayEquals(expected, actual);
    }

}
