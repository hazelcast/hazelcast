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
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAllNull;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.expression.ExpressionTestBase.TYPE_FACTORY;
import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastOperandTypesTest {

    @Test
    public void checkComparable() {
        //@formatter:off
        assertChecker(
                "1 = 1", COMPARABLE_ORDERED_COMPARABLE_ORDERED, true,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(TINYINT)
        );
        assertChecker(
                "1 = '1'", COMPARABLE_ORDERED_COMPARABLE_ORDERED, true,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(VARCHAR)
        );
        assertChecker(
                "1 = 1 /* boolean */", COMPARABLE_ORDERED_COMPARABLE_ORDERED, false,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(BOOLEAN)
        );
        //@formatter:on
    }

    @Test
    public void checkNotAny() {
        //@formatter:off
        assertChecker(
                "1 = 1", notAny(COMPARABLE_ORDERED_COMPARABLE_ORDERED), true,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(TINYINT)
        );
        assertChecker(
                "1 = '1'", notAny(COMPARABLE_ORDERED_COMPARABLE_ORDERED), true,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(VARCHAR)
        );
        assertChecker(
                "1 = 1 /* any */", notAny(COMPARABLE_ORDERED_COMPARABLE_ORDERED), false,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(SqlTypeName.ANY)
        );
        assertChecker(
                "1 = 1 /* boolean */", notAny(COMPARABLE_ORDERED_COMPARABLE_ORDERED), false,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(BOOLEAN)
        );
        //@formatter:on
    }

    @Test
    public void checkNotAllNull() {
        //@formatter:off
        assertChecker(
                "1 = null", notAllNull(COMPARABLE_ORDERED_COMPARABLE_ORDERED), true,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(NULL)
        );
        assertChecker(
                "null = 1", notAllNull(COMPARABLE_ORDERED_COMPARABLE_ORDERED), true,
                TYPE_FACTORY.createSqlType(NULL), TYPE_FACTORY.createSqlType(TINYINT)
        );
        assertChecker(
                "null = null", notAllNull(COMPARABLE_ORDERED_COMPARABLE_ORDERED), false,
                TYPE_FACTORY.createSqlType(NULL), TYPE_FACTORY.createSqlType(NULL)
        );
        assertChecker(
                "1 = 1 /* boolean */", notAllNull(COMPARABLE_ORDERED_COMPARABLE_ORDERED), false,
                TYPE_FACTORY.createSqlType(TINYINT), TYPE_FACTORY.createSqlType(BOOLEAN)
        );
        //@formatter:on
    }

    private static void assertChecker(String expression, SqlOperandTypeChecker checker, boolean expected, RelDataType... input) {
        SqlCallBinding binding = ExpressionTestBase.makeMockBinding(expression, input);

        assertEquals(expected, checker.checkOperandTypes(binding, false));
        if (!expected) {
            assertThrows(CalciteContextException.class, () -> checker.checkOperandTypes(binding, true));
        }
    }

}
