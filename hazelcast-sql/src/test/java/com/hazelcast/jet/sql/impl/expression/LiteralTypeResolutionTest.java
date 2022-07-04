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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.jet.sql.impl.validate.literal.Literal;
import com.hazelcast.jet.sql.impl.validate.literal.LiteralUtils;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Isolated tests for literal type resolution.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiteralTypeResolutionTest {
    @Test
    public void testNull() {
        checkLiteral(SqlLiteral.createNull(ZERO), null, null, NULL, true);
    }

    @Test
    public void testVarchar() {
        checkLiteral(SqlLiteral.createCharString("foo", ZERO), "foo", "foo", VARCHAR, false);
    }

    @Test
    public void testBoolean() {
        checkLiteral(SqlLiteral.createBoolean(true, ZERO), true, "true", BOOLEAN, false);
        checkLiteral(SqlLiteral.createBoolean(false, ZERO), false, "false", BOOLEAN, false);
        checkLiteral(SqlLiteral.createUnknown(ZERO), null, null, BOOLEAN, true);
    }

    @Test
    public void testInteger() {
        checkLiteral(exactNumeric(1), (byte) 1, "1", TINYINT, false);

        checkLiteral(exactNumeric(Byte.MAX_VALUE), Byte.MAX_VALUE, Byte.MAX_VALUE, TINYINT, false);
        checkLiteral(exactNumeric(Byte.MIN_VALUE), Byte.MIN_VALUE, Byte.MIN_VALUE, TINYINT, false);

        checkLiteral(exactNumeric(Short.MAX_VALUE), Short.MAX_VALUE, Short.MAX_VALUE, SMALLINT, false);
        checkLiteral(exactNumeric(Short.MIN_VALUE), Short.MIN_VALUE, Short.MIN_VALUE, SMALLINT, false);

        checkLiteral(exactNumeric(Integer.MAX_VALUE), Integer.MAX_VALUE, Integer.MAX_VALUE, INTEGER, false);
        checkLiteral(exactNumeric(Integer.MIN_VALUE), Integer.MIN_VALUE, Integer.MIN_VALUE, INTEGER, false);

        checkLiteral(exactNumeric(Long.MAX_VALUE), Long.MAX_VALUE, Long.MAX_VALUE, BIGINT, false);
        checkLiteral(exactNumeric(Long.MIN_VALUE), Long.MIN_VALUE, Long.MIN_VALUE, BIGINT, false);

        checkLiteral(exactNumeric("1.1"), new BigDecimal("1.1"), "1.1", DECIMAL, false);
        checkLiteral(exactNumeric(Long.MAX_VALUE + "0"), new BigDecimal(Long.MAX_VALUE + "0"), Long.MAX_VALUE + "0", DECIMAL, false);
        checkLiteral(exactNumeric(Long.MIN_VALUE + "0"), new BigDecimal(Long.MIN_VALUE + "0"), Long.MIN_VALUE + "0", DECIMAL, false);
    }

    @Test
    public void testApproxNumeric() {
        checkLiteral(SqlLiteral.createApproxNumeric("1.1E1", ZERO), 1.1E1, "1.1E1", DOUBLE, false);
    }

    @Test
    public void testDecimal() {
        BigDecimal maxPlusOne = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        checkLiteral(exactNumeric(maxPlusOne), maxPlusOne, maxPlusOne, DECIMAL, false);
    }

    private void checkLiteral(
            SqlLiteral literal,
            Object expectedValue,
            Object expectedStringValue,
            SqlTypeName expectedTypeName,
            boolean expectedNullable
    ) {
        String expectedStringValue0 = expectedStringValue != null ? expectedStringValue.toString() : null;

        Literal literal0 = LiteralUtils.literal(literal);
        assertNotNull(literal0);

        assertEquals(expectedValue, literal0.getValue());
        assertEquals(expectedStringValue0, literal0.getStringValue());
        assertEquals(expectedTypeName, literal0.getTypeName());
        assertEquals(expectedTypeName, literal0.getType(HazelcastTypeFactory.INSTANCE).getSqlTypeName());
        assertEquals(expectedNullable, literal0.getType(HazelcastTypeFactory.INSTANCE).isNullable());

        assertEquals(expectedTypeName, LiteralUtils.literalTypeName(literal));

        RelDataType type = LiteralUtils.literalType(literal, HazelcastTypeFactory.INSTANCE);
        assertNotNull(type);
        assertEquals(expectedTypeName, type.getSqlTypeName());
        assertEquals(expectedNullable, type.isNullable());
    }

    private static SqlLiteral exactNumeric(Object value) {
        return SqlLiteral.createExactNumeric(value.toString(), ZERO);
    }
}
