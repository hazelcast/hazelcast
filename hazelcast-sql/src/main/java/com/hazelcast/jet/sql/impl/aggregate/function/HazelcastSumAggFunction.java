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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;

public class HazelcastSumAggFunction extends HazelcastAggFunction {

    public HazelcastSumAggFunction() {
        super(
                "SUM",
                SqlKind.SUM,
                ReturnTypes.AGG_SUM,
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

        SqlTypeName resultTypeName;
        switch (operandType.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                resultTypeName = BIGINT;
                break;
            case BIGINT:
            case DECIMAL:
                resultTypeName = DECIMAL;
                break;
            case REAL:
                resultTypeName = REAL;
                break;
            case DOUBLE:
                resultTypeName = DOUBLE;
                break;

            default:
                throw new UnsupportedOperationException(operandType.getSqlTypeName().toString());
        }

        RelDataType resultType = HazelcastTypeFactory.INSTANCE.createSqlType(resultTypeName);
        TypedOperandChecker checker = TypedOperandChecker.forType(resultType);
        assert checker.isNumeric();
        return checker.check(binding, throwOnFailure, 0);
    }
}
