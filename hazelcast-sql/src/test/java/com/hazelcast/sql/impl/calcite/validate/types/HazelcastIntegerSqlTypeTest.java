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

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastIntegerSqlTypeTest {
    @Test
    public void testNullableIntegerTypeOfTypeName() {
        assertType(TINYINT, Byte.SIZE - 1, false, HazelcastIntegerSqlType.create(TINYINT, false));
        assertType(SMALLINT, Short.SIZE - 1, false, HazelcastIntegerSqlType.create(SMALLINT, false));
        assertType(INTEGER, Integer.SIZE - 1, false, HazelcastIntegerSqlType.create(INTEGER, false));
        assertType(BIGINT, Long.SIZE - 1, false, HazelcastIntegerSqlType.create(BIGINT, false));

        assertType(TINYINT, Byte.SIZE - 1, true, HazelcastIntegerSqlType.create(TINYINT, true));
        assertType(SMALLINT, Short.SIZE - 1, true, HazelcastIntegerSqlType.create(SMALLINT, true));
        assertType(INTEGER, Integer.SIZE - 1, true, HazelcastIntegerSqlType.create(INTEGER, true));
        assertType(BIGINT, Long.SIZE - 1, true, HazelcastIntegerSqlType.create(BIGINT, true));
    }

    @Test
    public void testNullableIntegerTypeOfType() {
        HazelcastIntegerSqlType intType = HazelcastIntegerSqlType.create(INTEGER, false);
        HazelcastIntegerSqlType nullableIntType = HazelcastIntegerSqlType.create(INTEGER, true);

        assertSame(intType, HazelcastIntegerSqlType.create(intType, false));
        assertSame(nullableIntType, HazelcastIntegerSqlType.create(intType, true));

        assertSame(intType, HazelcastIntegerSqlType.create(nullableIntType, false));
        assertSame(nullableIntType, HazelcastIntegerSqlType.create(nullableIntType, true));
    }

    @Test
    public void testNullableIntegerTypeOfBitWidth() {
        for (int i = 0; i < Long.SIZE + 10; ++i) {
            RelDataType type = HazelcastIntegerSqlType.create(i, false);
            RelDataType nullableType = HazelcastIntegerSqlType.create(i, true);

            if (i < Byte.SIZE) {
                assertType(TINYINT, i, false, type);
                assertType(TINYINT, i, true, nullableType);
            } else if (i < Short.SIZE) {
                assertType(SMALLINT, i, false, type);
                assertType(SMALLINT, i, true, nullableType);
            } else if (i < Integer.SIZE) {
                assertType(INTEGER, i, false, type);
                assertType(INTEGER, i, true, nullableType);
            } else if (i < Long.SIZE) {
                assertType(BIGINT, i, false, type);
                assertType(BIGINT, i, true, nullableType);
            } else {
                assertType(BIGINT, Long.SIZE, false, type);
                assertType(BIGINT, Long.SIZE, true, nullableType);
            }
        }
    }

    @Test
    public void testBitWidthOfLong() {
        assertEquals(0, HazelcastIntegerSqlType.bitWidthOf(0));
        assertEquals(1, HazelcastIntegerSqlType.bitWidthOf(1));
        assertEquals(1, HazelcastIntegerSqlType.bitWidthOf(-1));
        assertEquals(2, HazelcastIntegerSqlType.bitWidthOf(2));
        assertEquals(2, HazelcastIntegerSqlType.bitWidthOf(-2));
        assertEquals(10, HazelcastIntegerSqlType.bitWidthOf(555));
        assertEquals(10, HazelcastIntegerSqlType.bitWidthOf(-555));

        assertEquals(Long.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Long.MAX_VALUE));
        assertEquals(Long.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Long.MIN_VALUE));

        assertEquals(Integer.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Integer.MAX_VALUE));
        assertEquals(Integer.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Integer.MIN_VALUE));

        assertEquals(Short.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Short.MAX_VALUE));
        assertEquals(Short.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Short.MIN_VALUE));

        assertEquals(Byte.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Byte.MAX_VALUE));
        assertEquals(Byte.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(Byte.MIN_VALUE));
    }

    @Test
    public void testBitWidthOfTypeName() {
        for (SqlTypeName typeName : SqlTypeName.values()) {
            switch (typeName) {
                case TINYINT:
                    assertEquals(Byte.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(typeName));
                    break;

                case SMALLINT:
                    assertEquals(Short.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(typeName));
                    break;

                case INTEGER:
                    assertEquals(Integer.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(typeName));
                    break;

                case BIGINT:
                    assertEquals(Long.SIZE - 1, HazelcastIntegerSqlType.bitWidthOf(typeName));
                    break;

                default:
                    //noinspection ResultOfMethodCallIgnored
                    assertThrows(IllegalArgumentException.class, () -> HazelcastIntegerSqlType.bitWidthOf(typeName));
                    break;
            }
        }
    }

    private static void assertType(SqlTypeName expectedTypeName, int expectedBitWidth, boolean expectedNullable,
                                   RelDataType actual) {
        if (actual == null) {
            fail("non-null actual type expected");
        }

        assertEquals(expectedTypeName, actual.getSqlTypeName());
        assertEquals(expectedBitWidth, ((HazelcastIntegerSqlType) actual).getBitWidth());
        assertEquals(expectedNullable, actual.isNullable());
    }

}
