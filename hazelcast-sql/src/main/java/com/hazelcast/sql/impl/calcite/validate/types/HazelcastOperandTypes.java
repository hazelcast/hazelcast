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

import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastOperandTypes {

    // The same as Calcite's OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED,
    // but selects the least restrictive type as a common type. We do character
    // coercion provided by Consistency.COMPARE on our own.
    public static final SqlOperandTypeChecker COMPARABLE_ORDERED_COMPARABLE_ORDERED =
            new ComparableOperandTypeChecker(2, RelDataTypeComparability.ALL,
                    SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE);

    private HazelcastOperandTypes() {
    }

    public static SqlSingleOperandTypeChecker notAny(SqlOperandTypeChecker base) {
        return new NotAny(base);
    }

    /**
     * Disallows ANY type on operands while allowing all other types allowed by
     * the provided base operand type checker.
     */
    private static final class NotAny implements SqlSingleOperandTypeChecker {

        private final SqlOperandTypeChecker base;

        NotAny(SqlOperandTypeChecker base) {
            this.base = base;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!base.checkOperandTypes(callBinding, throwOnFailure)) {
                return false;
            }

            for (int i = 0; i < callBinding.getOperandCount(); ++i) {
                if (!checkSingleOperandType(callBinding, callBinding.operand(i), i, throwOnFailure)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return base.getOperandCountRange();
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return base.getAllowedSignatures(op, opName);
        }

        @Override
        public Consistency getConsistency() {
            return base.getConsistency();
        }

        @Override
        public boolean isOptional(int i) {
            return base.isOptional(i);
        }

        @Override
        public boolean checkSingleOperandType(SqlCallBinding callBinding, SqlNode operand, int iFormalOperand,
                                              boolean throwOnFailure) {
            if (callBinding.getOperandType(iFormalOperand).getSqlTypeName() == SqlTypeName.ANY) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }

            return true;
        }

    }

}
