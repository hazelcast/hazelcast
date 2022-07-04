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

package com.hazelcast.jet.sql.impl.validate.operators.predicate;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastPostfixOperator;
import com.hazelcast.jet.sql.impl.validate.param.NoOpParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;

public final class HazelcastIsTrueFalseNullPredicate extends HazelcastPostfixOperator {

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
                objectOperand ? OperandTypeInference.OBJECT : OperandTypeInference.BOOLEAN
        );

        this.objectOperand = objectOperand;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (objectOperand) {
            // IS (NOT) NULL accept any operand types
            SqlNode node = binding.operand(0);

            if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
                int parameterIndex = ((SqlDynamicParam) node).getIndex();

                binding.getValidator().setParameterConverter(parameterIndex, NoOpParameterConverter.INSTANCE);
            }

            return true;
        } else {
            // IS (NOT) TRUE|FALSE accept boolean operands only
            return TypedOperandChecker.BOOLEAN.check(binding, throwOnFailure, 0);
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
            for (int i = 0; i < operandTypes.length; i++) {
                RelDataType type = binding.getOperandType(i);

                if (isNullOrUnknown(type.getSqlTypeName())) {
                    if (objectOperand) {
                        type = HazelcastTypeUtils.createType(binding.getTypeFactory(), SqlTypeName.ANY, type.isNullable());
                    } else {
                        type = HazelcastTypeUtils.createType(binding.getTypeFactory(), SqlTypeName.BOOLEAN, type.isNullable());
                    }
                }

                operandTypes[i] = type;
            }
        }
    }
}
