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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.typeName;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

/**
 * A collection of operand type checkers. Basically, a mirror of {@link
 * OperandTypes} provided by Calcite with various enhancements.
 */
public final class HazelcastOperandTypes {

    /**
     * The same as Calcite's {@link OperandTypes#COMPARABLE_ORDERED_COMPARABLE_ORDERED},
     * but selects the least restrictive type as a common type. We do character
     * coercion provided by {@link SqlOperandTypeChecker.Consistency#COMPARE} and
     * used by Calcite on our own.
     */
    public static final SqlOperandTypeChecker COMPARABLE_ORDERED_COMPARABLE_ORDERED =
            new ComparableOperandTypeChecker(2, RelDataTypeComparability.ALL,
                    SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE);

    private HazelcastOperandTypes() {
    }

    /**
     * @return the base operand type checker wrapped into a new type checker
     * disallowing ANY type.
     */
    public static SqlSingleOperandTypeChecker notAny(SqlOperandTypeChecker base) {
        return new NotAny(base);
    }

    /**
     * @return the base operand type checker wrapped into a new type checker
     * disallowing all of the operands to be of NULL type simultaneously.
     */
    public static SqlOperandTypeChecker notAllNull(SqlOperandTypeChecker base) {
        return new NotAllNull(base);
    }

    private static final class NotAny implements SqlSingleOperandTypeChecker {

        private final SqlOperandTypeChecker base;

        NotAny(SqlOperandTypeChecker base) {
            this.base = base;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
            if (!base.checkOperandTypes(binding, throwOnFailure)) {
                return false;
            }

            for (int i = 0; i < binding.getOperandCount(); ++i) {
                if (!checkSingleOperandType(binding, binding.operand(i), i, throwOnFailure)) {
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
        public boolean checkSingleOperandType(SqlCallBinding binding, SqlNode operand, int index, boolean throwOnFailure) {
            if (typeName(binding.getOperandType(index)) == ANY) {
                if (throwOnFailure) {
                    throw binding.newValidationSignatureError();
                }
                return false;
            }

            return true;
        }

    }

    private static final class NotAllNull implements SqlOperandTypeChecker {

        private final SqlOperandTypeChecker base;

        NotAllNull(SqlOperandTypeChecker base) {
            this.base = base;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
            boolean seenNonNull = false;
            for (int i = 0; i < binding.getOperandCount(); ++i) {
                if (typeName(binding.getOperandType(i)) != NULL) {
                    seenNonNull = true;
                    break;
                }
            }

            if (!seenNonNull) {
                if (throwOnFailure) {
                    throw binding.newValidationSignatureError();
                }
                return false;
            }

            return base.checkOperandTypes(binding, throwOnFailure);
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

    }

}
