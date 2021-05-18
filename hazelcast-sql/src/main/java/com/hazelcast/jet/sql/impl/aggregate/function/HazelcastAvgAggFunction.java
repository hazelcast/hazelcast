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

package com.hazelcast.jet.sql.impl.aggregate.function;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastAggFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.isNumericIntegerType;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;

public class HazelcastAvgAggFunction extends HazelcastAggFunction {

    public HazelcastAvgAggFunction() {
        super(
                "AVG",
                SqlKind.AVG,
                ReturnTypes.AVG_AGG_FUNCTION,
                new ReplaceUnknownOperandTypeInference(BIGINT),
                null,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN);
    }

    protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        RelDataType operandType = binding.getOperandType(0);
        if (!HazelcastTypeUtils.isNumericType(operandType)) {
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        // we'll use DECIMAL for exact types (integers and DECIMAL) and DOUBLE for inexact types
        RelDataType resultType = isNumericIntegerType(operandType) || operandType.getSqlTypeName() == DECIMAL
                ? HazelcastTypeFactory.INSTANCE.createSqlType(DECIMAL)
                : HazelcastTypeFactory.INSTANCE.createSqlType(DOUBLE);

        TypedOperandChecker checker = TypedOperandChecker.forType(resultType);
        assert checker.isNumeric();
        return checker.check(binding, throwOnFailure, 0);
    }
}
