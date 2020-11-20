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

package com.hazelcast.sql.impl.calcite.validate.operators.misc;

import com.hazelcast.sql.impl.calcite.validate.operand.CompositeOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public final class HazelcastArithmeticOperator extends HazelcastBinaryOperator {

    public static final HazelcastArithmeticOperator PLUS = new HazelcastArithmeticOperator(SqlStdOperatorTable.PLUS);
    public static final HazelcastArithmeticOperator MINUS = new HazelcastArithmeticOperator(SqlStdOperatorTable.MINUS);
    public static final HazelcastArithmeticOperator MULTIPLY = new HazelcastArithmeticOperator(SqlStdOperatorTable.MULTIPLY);
    public static final HazelcastArithmeticOperator DIVIDE = new HazelcastArithmeticOperator(SqlStdOperatorTable.DIVIDE);

    private HazelcastArithmeticOperator(SqlBinaryOperator base) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            true,
            wrap(ReturnTypes.ARG0),
            new ReplaceUnknownOperandTypeInference(BIGINT) // TODO: Use custom inference, similar to comparisons
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.BINARY;
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        RelDataType firstType = binding.getOperandType(0);
        RelDataType secondType = binding.getOperandType(1);

        if (!isNumeric(firstType) || !isNumeric(secondType)) {
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        RelDataType type = HazelcastTypeSystem.withHigherPrecedence(firstType, secondType);

        switch (kind) {
            case PLUS:
            case MINUS:
                if (HazelcastTypeSystem.isInteger(type)) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(type) + 1;

                    type = HazelcastIntegerType.of(bitWidth, type.isNullable());
                }

                break;

            case TIMES:
                if (HazelcastTypeSystem.isInteger(firstType) && HazelcastTypeSystem.isInteger(secondType)) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(firstType) + HazelcastIntegerType.bitWidthOf(secondType);

                    type = HazelcastIntegerType.of(bitWidth, type.isNullable());
                }

                break;

            default:
                assert kind == SqlKind.DIVIDE;
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(type);

        return new CompositeOperandChecker(
            checker,
            checker
        ).check(binding, throwOnFailure);
    }

    private static boolean isNumeric(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }
}
