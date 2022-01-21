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
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.param.AnyToVarcharParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastConcatWSOperator extends HazelcastFunction {

    public static final HazelcastConcatWSOperator INSTANCE = new HazelcastConcatWSOperator();

    private HazelcastConcatWSOperator() {
        super(
                "CONCAT_WS",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED),
                        SqlTypeTransforms.TO_NULLABLE),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR),
                SqlFunctionCategory.STRING
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(2);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        HazelcastSqlValidator validator = binding.getValidator();

        if (binding.getOperandType(0).getSqlTypeName() != VARCHAR) {
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            }
            return false;
        }

        for (int i = 1; i < binding.getOperandCount(); i++) {
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
