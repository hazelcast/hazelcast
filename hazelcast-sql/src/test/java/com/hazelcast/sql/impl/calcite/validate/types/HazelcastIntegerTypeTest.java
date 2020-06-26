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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastIntegerTypeTest {

    @Test
    public void testIntegerTypeOfTypeName() {
        assertType(TINYINT, Byte.SIZE - 1, false, HazelcastIntegerType.of(TINYINT));
        assertType(SMALLINT, Short.SIZE - 1, false, HazelcastIntegerType.of(SMALLINT));
        assertType(INTEGER, Integer.SIZE - 1, false, HazelcastIntegerType.of(INTEGER));
        assertType(BIGINT, Long.SIZE - 1, false, HazelcastIntegerType.of(BIGINT));
    }

    @Test
    public void testNullableIntegerTypeOfTypeName() {
        assertType(TINYINT, Byte.SIZE - 1, false, HazelcastIntegerType.of(TINYINT, false));
        assertType(SMALLINT, Short.SIZE - 1, false, HazelcastIntegerType.of(SMALLINT, false));
        assertType(INTEGER, Integer.SIZE - 1, false, HazelcastIntegerType.of(INTEGER, false));
        assertType(BIGINT, Long.SIZE - 1, false, HazelcastIntegerType.of(BIGINT, false));

        assertType(TINYINT, Byte.SIZE - 1, true, HazelcastIntegerType.of(TINYINT, true));
        assertType(SMALLINT, Short.SIZE - 1, true, HazelcastIntegerType.of(SMALLINT, true));
        assertType(INTEGER, Integer.SIZE - 1, true, HazelcastIntegerType.of(INTEGER, true));
        assertType(BIGINT, Long.SIZE - 1, true, HazelcastIntegerType.of(BIGINT, true));
    }

    @Test
    public void testNullableIntegerTypeOfType() {
        RelDataType intType = HazelcastIntegerType.of(INTEGER);
        RelDataType nullableIntType = HazelcastIntegerType.of(INTEGER, true);

        assertSame(intType, HazelcastIntegerType.of(intType, false));
        assertSame(nullableIntType, HazelcastIntegerType.of(intType, true));

        assertSame(intType, HazelcastIntegerType.of(nullableIntType, false));
        assertSame(nullableIntType, HazelcastIntegerType.of(nullableIntType, true));
    }

    @Test
    public void testNullableIntegerTypeOfBitWidth() {
        for (int i = 0; i < Long.SIZE + 10; ++i) {
            RelDataType type = HazelcastIntegerType.of(i, false);
            RelDataType nullableType = HazelcastIntegerType.of(i, true);

            if (i < Byte.SIZE) {
                assertType(TINYINT, i, false, type);
                assertType(TINYINT, i, true, nullableType);

                assertFalse(HazelcastIntegerType.canOverflow(type));
                assertFalse(HazelcastIntegerType.canOverflow(nullableType));

                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(type));
                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(nullableType));
            } else if (i < Short.SIZE) {
                assertType(SMALLINT, i, false, type);
                assertType(SMALLINT, i, true, nullableType);

                assertFalse(HazelcastIntegerType.canOverflow(type));
                assertFalse(HazelcastIntegerType.canOverflow(nullableType));

                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(type));
                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(nullableType));
            } else if (i < Integer.SIZE) {
                assertType(INTEGER, i, false, type);
                assertType(INTEGER, i, true, nullableType);

                assertFalse(HazelcastIntegerType.canOverflow(type));
                assertFalse(HazelcastIntegerType.canOverflow(nullableType));

                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(type));
                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(nullableType));
            } else if (i < Long.SIZE) {
                assertType(BIGINT, i, false, type);
                assertType(BIGINT, i, true, nullableType);

                assertFalse(HazelcastIntegerType.canOverflow(type));
                assertFalse(HazelcastIntegerType.canOverflow(nullableType));

                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(type));
                assertEquals(i, HazelcastIntegerType.noOverflowBitWidthOf(nullableType));
            } else {
                assertType(BIGINT, Long.SIZE, false, type);
                assertType(BIGINT, Long.SIZE, true, nullableType);

                assertTrue(HazelcastIntegerType.canOverflow(type));
                assertTrue(HazelcastIntegerType.canOverflow(nullableType));

                assertEquals(Long.SIZE - 1, HazelcastIntegerType.noOverflowBitWidthOf(type));
                assertEquals(Long.SIZE - 1, HazelcastIntegerType.noOverflowBitWidthOf(nullableType));
            }
        }
    }

    @Test
    public void testSupports() {
        for (SqlTypeName typeName : SqlTypeName.values()) {
            assertEquals(typeName == TINYINT || typeName == SMALLINT || typeName == INTEGER || typeName == BIGINT,
                    HazelcastIntegerType.supports(typeName));
        }
    }

    @Test
    public void testBitWidthOfLong() {
        assertEquals(0, HazelcastIntegerType.bitWidthOf(0));
        assertEquals(1, HazelcastIntegerType.bitWidthOf(1));
        assertEquals(1, HazelcastIntegerType.bitWidthOf(-1));
        assertEquals(2, HazelcastIntegerType.bitWidthOf(2));
        assertEquals(2, HazelcastIntegerType.bitWidthOf(-2));
        assertEquals(10, HazelcastIntegerType.bitWidthOf(555));
        assertEquals(10, HazelcastIntegerType.bitWidthOf(-555));
        assertEquals(Long.SIZE - 1, HazelcastIntegerType.bitWidthOf(Long.MAX_VALUE));
        assertEquals(Long.SIZE - 1, HazelcastIntegerType.bitWidthOf(Long.MIN_VALUE));
    }

    @Test
    public void testBitWidthOfTypeName() {
        for (SqlTypeName typeName : SqlTypeName.values()) {
            switch (typeName) {
                case TINYINT:
                    assertEquals(Byte.SIZE - 1, HazelcastIntegerType.bitWidthOf(typeName));
                    break;

                case SMALLINT:
                    assertEquals(Short.SIZE - 1, HazelcastIntegerType.bitWidthOf(typeName));
                    break;

                case INTEGER:
                    assertEquals(Integer.SIZE - 1, HazelcastIntegerType.bitWidthOf(typeName));
                    break;

                case BIGINT:
                    assertEquals(Long.SIZE - 1, HazelcastIntegerType.bitWidthOf(typeName));
                    break;

                default:
                    //noinspection ResultOfMethodCallIgnored
                    assertThrows(IllegalArgumentException.class, () -> HazelcastIntegerType.bitWidthOf(typeName));
                    break;
            }
        }
    }

    @Test
    public void testDeriveLiteralType() {
        assertType(TINYINT, 0, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("0", ZERO)));
        assertType(TINYINT, 1, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("1", ZERO)));
        assertType(TINYINT, 1, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("-1", ZERO)));
        assertType(TINYINT, 2, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("2", ZERO)));
        assertType(TINYINT, 2, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("-2", ZERO)));
        assertType(SMALLINT, 10, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("555", ZERO)));
        assertType(SMALLINT, 10, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("-555", ZERO)));
        assertType(INTEGER, 16, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("55555", ZERO)));
        assertType(INTEGER, 16, false, HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric("-55555", ZERO)));
        assertType(BIGINT, Long.SIZE - 1, false,
                HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric(Long.toString(Long.MAX_VALUE), ZERO)));
        assertType(BIGINT, Long.SIZE - 1, false,
                HazelcastIntegerType.deriveLiteralType(SqlLiteral.createExactNumeric(Long.toString(Long.MIN_VALUE), ZERO)));

        assertThrows(Error.class, () -> HazelcastIntegerType.deriveLiteralType(SqlLiteral.createCharString("foo", ZERO)));
        assertThrows(Error.class, () -> HazelcastIntegerType.deriveLiteralType(SqlLiteral.createCharString("0", ZERO)));

        assertType(BIGINT, Long.SIZE - 1, false,
                HazelcastIntegerType.deriveLiteralType(SqlLiteral.createApproxNumeric("0.1", ZERO)));
    }

    @Test
    public void testCastTypeToType() {
        // tinyint(0) as tinyint(0) -> tinyint(0), identity
        assertType(TINYINT, 0, false,
                HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(0, false), HazelcastIntegerType.of(0, false)));

        // bigint(50) as bigint(50) -> bigint(50), identity
        assertType(BIGINT, 50, false,
                HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(50, false), HazelcastIntegerType.of(50, false)));

        // tinyint(1) as tinyint(5) -> tinyint(1), bit width is preserved
        assertType(TINYINT, 1, true,
                HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(1, false), HazelcastIntegerType.of(5, true)));

        // smallint(10) as int(20) -> int(10), type is upgraded, bit width is preserved
        assertType(INTEGER, 10, false,
                HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(10, true), HazelcastIntegerType.of(20, false)));

        // int(20) as smallint(10) -> smallint(16), overflow detected
        assertType(SMALLINT, Short.SIZE, false,
                HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(20, true), HazelcastIntegerType.of(10, false)));

        // bigint(64) as smallint(10) -> smallint(16), overflow preserved
        assertType(SMALLINT, Short.SIZE, false, HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(Long.SIZE, true),
                HazelcastIntegerType.of(10, false)));

        // bigint(64) as bigint(50) -> bigint(64), overflow preserved
        assertType(BIGINT, Long.SIZE, false, HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(Long.SIZE, true),
                HazelcastIntegerType.of(50, false)));

        // bigint(50) as bigint(40) -> bigint(50), no overflow, bigint(40) still can fit any bigint
        assertType(BIGINT, 50, false,
                HazelcastIntegerType.deriveCastType(HazelcastIntegerType.of(50, true), HazelcastIntegerType.of(40, false)));
    }

    @Test
    public void testCastLongToType() {
        assertType(TINYINT, 0, false, HazelcastIntegerType.deriveCastType(0, HazelcastIntegerType.of(5, false)));
        assertType(TINYINT, 1, false, HazelcastIntegerType.deriveCastType(1, HazelcastIntegerType.of(5, false)));
        assertType(TINYINT, Byte.SIZE, false, HazelcastIntegerType.deriveCastType(555, HazelcastIntegerType.of(5, false)));

        assertType(TINYINT, 0, true, HazelcastIntegerType.deriveCastType(0, HazelcastIntegerType.of(5, true)));
        assertType(TINYINT, 1, true, HazelcastIntegerType.deriveCastType(1, HazelcastIntegerType.of(5, true)));
        assertType(TINYINT, Byte.SIZE, true, HazelcastIntegerType.deriveCastType(555, HazelcastIntegerType.of(5, true)));
    }

    @Test
    public void testLeastRestrictive() {
        List<RelDataType> types = Arrays.asList(HazelcastIntegerType.of(5, true), HazelcastIntegerType.of(15, false),
                HazelcastIntegerType.of(7, false));
        RelDataType selected = HazelcastIntegerType.leastRestrictive(HazelcastIntegerType.of(1, true), types);
        assertType(TINYINT, 7, true, selected);
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
