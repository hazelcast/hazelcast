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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class NumericOperandChecker implements OperandChecker {

    public static final NumericOperandChecker UNKNOWN_AS_BIGINT = new NumericOperandChecker(SqlTypeName.BIGINT);
    public static final NumericOperandChecker UNKNOWN_AS_DECIMAL = new NumericOperandChecker(SqlTypeName.DECIMAL);

    private final SqlTypeName unknownTypeReplacement;

    private NumericOperandChecker(SqlTypeName unknownTypeReplacement) {
        this.unknownTypeReplacement = unknownTypeReplacement;
    }

    @Override
    public boolean check(SqlCallBindingOverride binding, boolean throwOnFailure, int index) {
        // Resolve a numeric checker for the operand
        SqlNode operand = binding.operand(index);

        RelDataType operandType = binding.getValidator().deriveType(binding.getScope(), operand);

        NumericTypedOperandChecker checker = checkerForTypeName(operandType.getSqlTypeName(), unknownTypeReplacement);

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
    private static NumericTypedOperandChecker checkerForTypeName(SqlTypeName typeName, SqlTypeName unknownTypeReplacement) {
        if (typeName == SqlTypeName.NULL) {
            typeName = unknownTypeReplacement;
        }

        switch (typeName) {
            case TINYINT:
                return NumericTypedOperandChecker.TINYINT;

            case SMALLINT:
                return NumericTypedOperandChecker.SMALLINT;

            case INTEGER:
                return NumericTypedOperandChecker.INTEGER;

            case BIGINT:
                return NumericTypedOperandChecker.BIGINT;

            case DECIMAL:
                return NumericTypedOperandChecker.DECIMAL;

            case REAL:
            case FLOAT:
                return NumericTypedOperandChecker.REAL;

            case DOUBLE:
                return NumericTypedOperandChecker.DOUBLE;

            default:
                return null;
        }
    }
}
