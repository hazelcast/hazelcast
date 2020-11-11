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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeCoercion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class HazelcastNotPrefixOperator extends SqlPrefixOperator implements HazelcastOperatorCoercion, SqlCallBindingManualOverride {
    public HazelcastNotPrefixOperator() {
        super(
            "NOT",
            SqlKind.NOT,
            SqlStdOperatorTable.NOT.getLeftPrec(),
            ReturnTypes.ARG0,
            InferTypes.BOOLEAN,
            null
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        callBinding = new SqlCallBindingOverride(callBinding);

        HazelcastSqlValidator validator = (HazelcastSqlValidator) callBinding.getValidator();

        SqlNode operand = callBinding.operand(0);

        if (operand.getKind() == SqlKind.LITERAL) {
            SqlLiteral literal = (SqlLiteral) operand;

            BiTuple<Boolean, Boolean> value = SqlNodeUtil.booleanValue(literal);

            if (value.element1()) {
                if (literal.getTypeName() != SqlTypeName.BOOLEAN) {
                    if (validator.isTypeCoercionEnabled()) {
                        boolean nullable = value.element2() == null;

                        if (nullable) {
                            literal = SqlLiteral.createUnknown(literal.getParserPosition());
                        } else {
                            literal = SqlLiteral.createBoolean(value.element2(), literal.getParserPosition());
                        }

                        RelDataType booleanType = SqlNodeUtil.createType(
                            validator.getTypeFactory(),
                            SqlTypeName.BOOLEAN,
                            nullable
                        );

                        callBinding.getCall().setOperand(0, literal);

                        validator.setKnownNodeType(literal, booleanType);
                    }
                }

                return true;
            } else {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        } else if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
            RelDataType booleanType = SqlNodeUtil.createType(validator.getTypeFactory(), SqlTypeName.BOOLEAN, true);

            validator.setKnownNodeType(operand, booleanType);

            return true;
        } else {
            RelDataType operandType = validator.deriveType(callBinding.getScope(), operand);

            if (operandType.getSqlTypeName() == SqlTypeName.BOOLEAN) {
                return true;
            } else {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public boolean coerce(HazelcastTypeCoercion coercion, SqlCallBinding binding, List<RelDataType> operandTypes) {
        RelDataType type = binding.getOperandType(0);

        return false;
    }
}
