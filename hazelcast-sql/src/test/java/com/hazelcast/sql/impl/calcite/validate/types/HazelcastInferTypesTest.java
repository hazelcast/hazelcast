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
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes.FIRST_KNOWN;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType.bitWidthOf;
import static com.hazelcast.sql.impl.expression.ExpressionTestBase.TYPE_FACTORY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastInferTypesTest {

    @Test
    public void testFirstKnown() {
        //@formatter:off
        assertInference(
                "1 = 1", FIRST_KNOWN,
                HazelcastIntegerType.of(1, false), HazelcastIntegerType.of(1, false),
                HazelcastIntegerType.of(1, false), HazelcastIntegerType.of(1, false)
        );
        //@formatter:on

        //@formatter:off
        assertInference(
                "null = 10", FIRST_KNOWN,
                TYPE_FACTORY.createSqlType(NULL), HazelcastIntegerType.of(bitWidthOf(10), false),
                HazelcastIntegerType.of(bitWidthOf(10), false), HazelcastIntegerType.of(bitWidthOf(10), false)
        );
        assertInference(
                "10 = null", FIRST_KNOWN,
                HazelcastIntegerType.of(bitWidthOf(10), false), TYPE_FACTORY.createSqlType(NULL),
                HazelcastIntegerType.of(bitWidthOf(10), false), HazelcastIntegerType.of(bitWidthOf(10), false)
        );
        assertInference(
                "null = null", FIRST_KNOWN,
                TYPE_FACTORY.createSqlType(NULL), TYPE_FACTORY.createSqlType(NULL),
                null, null
        );
        //@formatter:on

        //@formatter:off
        assertInference(
                "1 = ?", FIRST_KNOWN,
                TYPE_FACTORY.createSqlType(TINYINT), null,
                HazelcastIntegerType.of(BIGINT, false), HazelcastIntegerType.of(BIGINT, false)
        );
        assertInference(
                "? = 1", FIRST_KNOWN,
                null, TYPE_FACTORY.createSqlType(TINYINT),
                HazelcastIntegerType.of(BIGINT, false), HazelcastIntegerType.of(BIGINT, false)
        );
        //@formatter:on
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertInference(String expression, SqlOperandTypeInference inference, RelDataType... inputAndExpected) {
        assert inputAndExpected.length % 2 == 0;
        int length = inputAndExpected.length / 2;

        RelDataType[] input = new RelDataType[length];
        System.arraycopy(inputAndExpected, 0, input, 0, length);

        SqlCallBinding binding = ExpressionTestBase.makeMockBinding(expression, input);
        RelDataType unknown = binding.getValidator().getUnknownType();

        RelDataType[] actual = new RelDataType[length];
        Arrays.fill(actual, unknown);

        inference.inferOperandTypes(binding, unknown, actual);

        RelDataType[] expected = new RelDataType[length];
        System.arraycopy(inputAndExpected, length, expected, 0, length);
        for (int i = 0; i < expected.length; ++i) {
            if (expected[i] == null) {
                expected[i] = unknown;
            }
        }
        Assert.assertArrayEquals(expected, actual);
    }

}
