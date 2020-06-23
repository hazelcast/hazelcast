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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.CAST;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CastTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(CAST, CastTest::expectedTypes, CastTest::expectedValues, "CAST(%s AS %s)", ALL, TYPES);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];
        RelDataType from = operand.type;

        assert operands[1].isType();
        RelDataType to = (RelDataType) operands[1].value;
        // Calcite treats target types as NOT NULL
        assert !to.isNullable();

        // Handle NULL.

        if (isNull(from)) {
            return new RelDataType[]{from, to, TYPE_FACTORY.createTypeWithNullability(to, true)};
        }

        RelDataType returnType = to;

        // Assign type for parameters.

        if (operand.isParameter()) {
            from = TYPE_FACTORY.createTypeWithNullability(to, true);
        }

        // Assign type to numeric literals.

        BigDecimal numeric = operand.numericValue();

        if (isNumeric(to) || isNumeric(from)) {
            //noinspection NumberEquality
            if (numeric == INVALID_NUMERIC_VALUE) {
                return null;
            }

            if (numeric != null) {
                from = narrowestTypeFor(numeric, typeName(to));
            }
        }

        // Validate the cast.

        if (!HazelcastTypeSystem.canCast(from, to)) {
            return null;
        }

        if (operand.isLiteral() && !canCastLiteral(operand, from, to)) {
            return null;
        }

        // Infer the return type.

        if (isInteger(to) && isInteger(from)) {
            returnType = HazelcastIntegerType.deriveCastType(from, to);
        } else if (isInteger(to) && numeric != null) {
            // numeric value is already validated above and fits into long
            returnType = HazelcastIntegerType.deriveCastType(numeric.longValue(), to);
        }

        returnType = TYPE_FACTORY.createTypeWithNullability(returnType, from.isNullable());
        return new RelDataType[]{from, to, returnType};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        Object arg = args[0];
        RelDataType from = types[0];
        RelDataType to = types[1];

        if (arg == null) {
            return null;
        }

        Converter fromConverter = SqlToQueryType.map(from.getSqlTypeName()).getConverter();
        Converter toConverter = SqlToQueryType.map(to.getSqlTypeName()).getConverter();

        try {
            return toConverter.convertToSelf(fromConverter, arg);
        } catch (QueryException e) {
            assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
            return INVALID_VALUE;
        }
    }

}
