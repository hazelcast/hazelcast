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

package com.hazelcast.jet.sql.impl.validate.operators.string;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastBinaryOperator;
import com.hazelcast.jet.sql.impl.validate.param.AnyToVarcharParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
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
