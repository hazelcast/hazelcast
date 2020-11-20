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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastObjectType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.NULL;

public final class HazelcastIsTrueFalseNullPredicate extends SqlPostfixOperator implements SqlCallBindingManualOverride {

    public static final HazelcastIsTrueFalseNullPredicate IS_TRUE =
        new HazelcastIsTrueFalseNullPredicate(SqlStdOperatorTable.IS_TRUE, false);

    public static final HazelcastIsTrueFalseNullPredicate IS_FALSE =
        new HazelcastIsTrueFalseNullPredicate(SqlStdOperatorTable.IS_FALSE, false);

    public static final HazelcastIsTrueFalseNullPredicate IS_NOT_TRUE =
        new HazelcastIsTrueFalseNullPredicate(SqlStdOperatorTable.IS_NOT_TRUE, false);

    public static final HazelcastIsTrueFalseNullPredicate IS_NOT_FALSE =
        new HazelcastIsTrueFalseNullPredicate(SqlStdOperatorTable.IS_NOT_FALSE, false);

    public static final HazelcastIsTrueFalseNullPredicate IS_NULL =
        new HazelcastIsTrueFalseNullPredicate(SqlStdOperatorTable.IS_NULL, true);

    public static final HazelcastIsTrueFalseNullPredicate IS_NOT_NULL =
        new HazelcastIsTrueFalseNullPredicate(SqlStdOperatorTable.IS_NOT_NULL, true);

    private final boolean objectOperand;

    private HazelcastIsTrueFalseNullPredicate(SqlPostfixOperator base, boolean objectOperand) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            ReturnTypes.BOOLEAN_NOT_NULL,
            objectOperand ? OperandTypeInference.OBJECT : OperandTypeInference.BOOLEAN,
            null
        );

        this.objectOperand = objectOperand;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (objectOperand) {
            // IS (NOT) NULL accept any operand types
            return true;
        } else {
            // IS (NOT) TRUE|FALSE accept boolean operands only
            return TypedOperandChecker.BOOLEAN.check(new SqlCallBindingOverride(callBinding), throwOnFailure, 0);
        }
    }

    private static final class OperandTypeInference implements SqlOperandTypeInference {

        private static final OperandTypeInference BOOLEAN = new OperandTypeInference(false);
        private static final OperandTypeInference OBJECT = new OperandTypeInference(true);

        private final boolean objectOperand;

        private OperandTypeInference(boolean objectOperand) {
            this.objectOperand = objectOperand;
        }

        @Override
        public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
            for (int i = 0; i < operandTypes.length; ++i) {
                RelDataType type = binding.getOperandType(i);

                if (type.getSqlTypeName() == NULL) {
                    RelDataType newType;

                    if (objectOperand) {
                        newType = HazelcastObjectType.NULLABLE_INSTANCE;
                    } else {
                        newType = SqlNodeUtil.createType(binding.getTypeFactory(), SqlTypeName.BOOLEAN, type.isNullable());
                    }

                    operandTypes[i] = newType;
                }
            }
        }
    }
}
