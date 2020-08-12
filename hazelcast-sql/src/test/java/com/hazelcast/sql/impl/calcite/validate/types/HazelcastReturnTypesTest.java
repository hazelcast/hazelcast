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
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes.binaryIntegerMinus;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes.binaryIntegerPlus;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes.integerDivide;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes.integerMultiply;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes.integerUnaryMinus;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastReturnTypesTest {

    @Test
    public void testBinaryIntegerPlus() {
        assertType(TINYINT, 0, false, binaryIntegerPlus(integer(0, false), integer(0, false)));
        assertType(TINYINT, 0, true, binaryIntegerPlus(integer(0, false), integer(0, true)));

        assertType(TINYINT, 1, false, binaryIntegerPlus(integer(1, false), integer(0, false)));
        assertType(TINYINT, 1, true, binaryIntegerPlus(integer(0, true), integer(1, false)));

        assertType(TINYINT, 6, false, binaryIntegerPlus(integer(5, false), integer(3, false)));
        assertType(TINYINT, 6, true, binaryIntegerPlus(integer(3, true), integer(5, false)));

        assertType(BIGINT, 32, false, binaryIntegerPlus(integer(31, false), integer(3, false)));
        assertType(BIGINT, 32, true, binaryIntegerPlus(integer(3, true), integer(31, false)));

        assertType(BIGINT, 64, false, binaryIntegerPlus(integer(100, false), integer(5, false)));
        assertType(BIGINT, 64, true, binaryIntegerPlus(integer(5, true), integer(100, false)));
    }

    @Test
    public void testBinaryIntegerMinus() {
        assertType(TINYINT, 0, false, binaryIntegerMinus(integer(0, false), integer(0, false)));
        assertType(TINYINT, 0, true, binaryIntegerMinus(integer(0, false), integer(0, true)));

        assertType(TINYINT, 1, false, binaryIntegerMinus(integer(1, false), integer(0, false)));
        assertType(TINYINT, 2, true, binaryIntegerMinus(integer(0, true), integer(1, false)));

        assertType(TINYINT, 7, false, binaryIntegerMinus(integer(5, false), integer(3, false)));
        assertType(TINYINT, 7, true, binaryIntegerMinus(integer(3, true), integer(5, false)));

        assertType(BIGINT, 33, false, binaryIntegerMinus(integer(31, false), integer(3, false)));
        assertType(BIGINT, 33, true, binaryIntegerMinus(integer(3, true), integer(31, false)));

        assertType(BIGINT, 64, false, binaryIntegerMinus(integer(100, false), integer(5, false)));
        assertType(BIGINT, 64, true, binaryIntegerMinus(integer(5, true), integer(100, false)));
    }

    @Test
    public void testIntegerMultiply() {
        assertType(TINYINT, 0, false, integerMultiply(integer(0, false), integer(0, false)));
        assertType(TINYINT, 0, true, integerMultiply(integer(0, false), integer(0, true)));

        assertType(TINYINT, 0, false, integerMultiply(integer(1, false), integer(0, false)));
        assertType(TINYINT, 0, true, integerMultiply(integer(0, true), integer(1, false)));

        assertType(SMALLINT, 8, false, integerMultiply(integer(5, false), integer(3, false)));
        assertType(SMALLINT, 8, true, integerMultiply(integer(3, true), integer(5, false)));

        assertType(BIGINT, 34, false, integerMultiply(integer(31, false), integer(3, false)));
        assertType(BIGINT, 34, true, integerMultiply(integer(3, true), integer(31, false)));

        assertType(BIGINT, 64, false, integerMultiply(integer(100, false), integer(5, false)));
        assertType(BIGINT, 64, true, integerMultiply(integer(5, true), integer(100, false)));
    }

    @Test
    public void testIntegerDivide() {
        assertType(TINYINT, 0, false, integerDivide(integer(0, false), integer(0, false)));
        assertType(TINYINT, 0, true, integerDivide(integer(0, false), integer(0, true)));

        assertType(TINYINT, 2, false, integerDivide(integer(1, false), integer(0, false)));
        assertType(TINYINT, 0, true, integerDivide(integer(0, true), integer(1, false)));

        assertType(TINYINT, 6, false, integerDivide(integer(5, false), integer(3, false)));
        assertType(TINYINT, 4, true, integerDivide(integer(3, true), integer(5, false)));

        assertType(BIGINT, 32, false, integerDivide(integer(31, false), integer(3, false)));
        assertType(TINYINT, 4, true, integerDivide(integer(3, true), integer(31, false)));

        assertType(BIGINT, 64, false, integerDivide(integer(100, false), integer(5, false)));
        assertType(TINYINT, 6, true, integerDivide(integer(5, true), integer(100, false)));
    }

    @Test
    public void testIntegerUnaryMinus() {
        assertType(TINYINT, 0, false, integerUnaryMinus(integer(0, false)));
        assertType(TINYINT, 0, true, integerUnaryMinus(integer(0, true)));

        assertType(TINYINT, 1, false, integerUnaryMinus(integer(1, false)));
        assertType(TINYINT, 1, true, integerUnaryMinus(integer(1, true)));

        assertType(SMALLINT, 8, false, integerUnaryMinus(integer(7, false)));
        assertType(SMALLINT, 8, true, integerUnaryMinus(integer(7, true)));

        assertType(BIGINT, 64, false, integerUnaryMinus(integer(100, false)));
        assertType(BIGINT, 64, true, integerUnaryMinus(integer(100, true)));
    }

    private static RelDataType integer(int bitWidth, boolean nullable) {
        return HazelcastIntegerType.of(bitWidth, nullable);
    }

    private static void assertType(SqlTypeName expectedTypeName, int expectedBitWidth, boolean expectedNullable,
                                   RelDataType actual) {
        if (actual == null) {
            fail("non-null actual type expected");
        }

        assertEquals(expectedTypeName, actual.getSqlTypeName());
        assertEquals(expectedBitWidth, HazelcastIntegerType.bitWidthOf(actual));
        assertEquals(expectedNullable, actual.isNullable());
    }

}
