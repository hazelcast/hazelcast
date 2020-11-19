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

package com.hazelcast.sql.impl.calcite.validate.operators.string;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.param.VarcharParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.wrap;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastConcatFunction extends SqlBinaryOperator implements SqlCallBindingManualOverride {

    public static final HazelcastConcatFunction INSTANCE = new HazelcastConcatFunction();

    private HazelcastConcatFunction() {
        super(
            "||",
            SqlKind.OTHER,
            SqlStdOperatorTable.CONCAT.getLeftPrec(),
            true,
            ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
            new ReplaceUnknownOperandTypeInference(VARCHAR),
            wrap(notAny(OperandTypes.STRING_SAME_SAME))
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        SqlCallBindingOverride bindingOverride = new SqlCallBindingOverride(binding);

        HazelcastSqlValidator validator = (HazelcastSqlValidator) binding.getValidator();

        for (int i = 0; i < bindingOverride.getOperandCount(); i++) {
            SqlNode operand = bindingOverride.operand(i);
            RelDataType operandType = bindingOverride.getOperandType(i);

            if (operandType.getSqlTypeName() != VARCHAR) {
                // Coerce everything to VARCHAR
                RelDataType newOperandType = SqlNodeUtil.createType(
                    validator.getTypeFactory(),
                    VARCHAR,
                    operandType.isNullable()
                );

                validator.getTypeCoercion().coerceOperandType(
                    bindingOverride.getScope(),
                    bindingOverride.getCall(),
                    i,
                    newOperandType
                );

                validator.setKnownAndValidatedNodeType(bindingOverride.operand(i), newOperandType);
            }

            // Set parameter converters
            if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
                int paramIndex = ((SqlDynamicParam) operand).getIndex();

                ParameterConverter paramConverter = new VarcharParameterConverter(paramIndex, operand.getParserPosition());

                validator.setParameterConverter(paramIndex, paramConverter);
            }
        }

        return true;
    }
}
