/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.param.AnyToVarcharParameterConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastConcatOperator extends HazelcastBinaryOperator {

    public static final HazelcastConcatOperator INSTANCE = new HazelcastConcatOperator();

    private HazelcastConcatOperator() {
        super(
            "||",
            SqlKind.OTHER,
            SqlStdOperatorTable.CONCAT.getLeftPrec(),
            true,
            ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
            new ReplaceUnknownOperandTypeInference(VARCHAR)
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        HazelcastSqlValidator validator = binding.getValidator();

        for (int i = 0; i < binding.getOperandCount(); i++) {
            SqlNode operand = binding.operand(i);
            RelDataType operandType = binding.getOperandType(i);

            if (operandType.getSqlTypeName() != VARCHAR) {
                // Coerce everything to VARCHAR
                RelDataType newOperandType = HazelcastTypeUtils.createType(
                    validator.getTypeFactory(),
                    VARCHAR,
                    operandType.isNullable()
                );

                validator.getTypeCoercion().coerceOperandType(
                    binding.getScope(),
                    binding.getCall(),
                    i,
                    newOperandType
                );
            }

            // Set parameter converters
            if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
                int paramIndex = ((SqlDynamicParam) operand).getIndex();

                ParameterConverter paramConverter = new AnyToVarcharParameterConverter(paramIndex, operand.getParserPosition());

                validator.setParameterConverter(paramIndex, paramConverter);
            }
        }

        return true;
    }
}
