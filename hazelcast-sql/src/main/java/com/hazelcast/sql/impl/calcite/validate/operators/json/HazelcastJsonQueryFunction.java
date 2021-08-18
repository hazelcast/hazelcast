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

package com.hazelcast.sql.impl.calcite.validate.operators.json;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastJsonType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

@SuppressWarnings("checkstyle:MagicNumber")
public class HazelcastJsonQueryFunction extends HazelcastFunction {
    public static final HazelcastJsonQueryFunction INSTANCE = new HazelcastJsonQueryFunction();

    public HazelcastJsonQueryFunction() {
        super(
                "JSON_QUERY",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HazelcastJsonType.create(true),
                new OperandTypeInference(),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return checkFirstOperand(callBinding, throwOnFailure)
                && TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 1)
                && TypedOperandChecker.SYMBOL.check(callBinding, throwOnFailure, 2)
                && TypedOperandChecker.SYMBOL.check(callBinding, throwOnFailure, 3)
                && TypedOperandChecker.SYMBOL.check(callBinding, throwOnFailure, 4);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 5);
    }

    private boolean checkFirstOperand(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        final RelDataType operandType = callBinding.getOperandType(0);
        if (operandType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 0);
        }

        if (operandType.getSqlTypeName().equals(SqlTypeName.OTHER)) {
            return TypedOperandChecker.JSON.check(callBinding, throwOnFailure, 0);
        }

        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }

    private static final class OperandTypeInference implements SqlOperandTypeInference {

        @Override
        public void inferOperandTypes(final SqlCallBinding callBinding,
                                      final RelDataType returnType,
                                      final RelDataType[] operandTypes) {
            final RelDataType firstOperandType = callBinding.getOperandType(0);

            operandTypes[0] = firstOperandType.getSqlTypeName().equals(SqlTypeName.OTHER)
                    ? HazelcastJsonType.create(firstOperandType.isNullable())
                    : callBinding.getTypeFactory().createSqlType(firstOperandType.getSqlTypeName());

            for (int i = 1; i < 5; i++) {
                operandTypes[i] = callBinding.getTypeFactory()
                        .createSqlType(callBinding.getOperandType(i).getSqlTypeName());
            }
        }
    }
}
