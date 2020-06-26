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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.impl.expression.ExpressionTestBase;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.UNARY_PLUS;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnaryPlusTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(UNARY_PLUS, UnaryPlusTest::expectedTypes, UnaryPlusTest::expectedValues, ALL);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];

        if (operand.isParameter()) {
            return null;
        }

        if (operand.typeName() == NULL) {
            return null;
        }

        if (operand.typeName() == ANY) {
            return null;
        }

        if (operand.typeName() == BOOLEAN) {
            return null;
        }

        RelDataType type = operand.type;

        BigDecimal numeric = operand.numericValue();
        //noinspection NumberEquality
        if (numeric == INVALID_NUMERIC_VALUE) {
            return null;
        }

        if (numeric != null) {
            type = narrowestTypeFor(numeric, null);
        } else if (isChar(type)) {
            type = TYPE_FACTORY.createSqlType(DOUBLE, type.isNullable());
        }

        if (operand.isLiteral() && !canRepresentLiteral(operand, type)) {
            return null;
        }

        return new RelDataType[]{type, type};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        Object arg = args[0];
        RelDataType type = types[0];
        assertEquals(type, types[1]);

        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == NULL) {
            return null;
        }

        if (arg == INVALID_VALUE) {
            return INVALID_VALUE;
        }
        if (arg == null) {
            return null;
        }

        switch (type.getSqlTypeName()) {
            case TINYINT:
                return number(arg).byteValue();
            case SMALLINT:
                return number(arg).shortValue();
            case INTEGER:
                return number(arg).intValue();
            case BIGINT:
                return number(arg).longValue();
            case REAL:
                return number(arg).floatValue();
            case DOUBLE:
                return number(arg).doubleValue();
            case DECIMAL:
                HazelcastTestSupport.assertInstanceOf(BigDecimal.class, arg);
                return arg;
            default:
                throw new IllegalArgumentException("unexpected type name: " + typeName);
        }
    }

}
