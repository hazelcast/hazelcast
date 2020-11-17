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

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.param.NumericPrecedenceParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class DoubleOperandChecker extends AbstractTypedOperandChecker {

    public static final DoubleOperandChecker INSTANCE = new DoubleOperandChecker();

    private DoubleOperandChecker() {
        super(SqlTypeName.DOUBLE);
    }

    @Override
    protected ParameterConverter parameterConverter(SqlDynamicParam operand) {
        return new NumericPrecedenceParameterConverter(
            operand.getIndex(),
            operand.getParserPosition(),
            QueryDataType.DOUBLE
        );
    }

    @Override
    protected boolean coerce(
        HazelcastSqlValidator validator,
        SqlCallBindingOverride callBinding,
        SqlNode operand,
        RelDataType operandType,
        int operandIndex
    ) {
        if (SqlToQueryType.map(operandType.getSqlTypeName()).getTypeFamily().isNumeric()) {
            RelDataType type = SqlNodeUtil.createType(validator.getTypeFactory(), typeName, operandType.isNullable());

            // TODO: Can it fail?
            validator.getTypeCoercion().coerceOperandType(callBinding.getScope(), callBinding.getCall(), operandIndex, type);
            validator.setKnownAndValidatedNodeType(operand, type);

            return true;
        }

        return false;
    }
}
