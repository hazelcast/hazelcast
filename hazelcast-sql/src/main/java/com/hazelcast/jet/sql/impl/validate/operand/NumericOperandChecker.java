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

package com.hazelcast.jet.sql.impl.validate.operand;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class NumericOperandChecker implements OperandChecker {

    public static final NumericOperandChecker INSTANCE = new NumericOperandChecker();

    private NumericOperandChecker() {
        // No-op.
    }

    @Override
    public boolean check(HazelcastCallBinding binding, boolean throwOnFailure, int index) {
        // Resolve a numeric checker for the operand
        SqlNode operand = binding.getCall().operand(index);

        RelDataType operandType = binding.getValidator().deriveType(binding.getScope(), operand);

        TypedOperandChecker checker = checkerForTypeName(operandType.getSqlTypeName());

        if (checker != null) {
            // Numeric checker is found, invoke
            return checker.check(binding, throwOnFailure, index);
        } else {
            // Not a numeric type, fail.
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static TypedOperandChecker checkerForTypeName(SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
                return TypedOperandChecker.TINYINT;

            case SMALLINT:
                return TypedOperandChecker.SMALLINT;

            case INTEGER:
                return TypedOperandChecker.INTEGER;

            case BIGINT:
                return TypedOperandChecker.BIGINT;

            case DECIMAL:
                return TypedOperandChecker.DECIMAL;

            case REAL:
            case FLOAT:
                return TypedOperandChecker.REAL;

            case DOUBLE:
                return TypedOperandChecker.DOUBLE;

            default:
                return null;
        }
    }
}
