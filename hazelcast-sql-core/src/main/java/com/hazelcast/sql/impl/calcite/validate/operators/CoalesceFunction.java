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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;
import static org.apache.calcite.util.Static.RESOURCE;

public final class CoalesceFunction extends HazelcastFunction {
    public static final CoalesceFunction INSTANCE = new CoalesceFunction();

    private CoalesceFunction() {
        super(
                "COALESCE",
                SqlKind.COALESCE,
                wrap(ReturnTypes.ARG0_NULLABLE),
                VariableLengthOperandTypeInference.INSTANCE,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        HazelcastSqlValidator validator = callBinding.getValidator();
        SqlValidatorScope scope = callBinding.getScope();

        SqlCall call = callBinding.getCall();
        List<SqlNode> operandList = call.getOperandList();

        List<RelDataType> argTypes = new ArrayList<>(operandList.size());

        boolean foundNotNull = false;
        for (SqlNode node : operandList) {
            argTypes.add(validator.deriveType(scope, node));
            foundNotNull |= !SqlUtil.isNullLiteral(node, false);
        }

        if (!foundNotNull) {
            if (throwOnFailure) {
                throw callBinding.newError(RESOURCE.mustNotNullInElse());
            }
            return false;
        }

        RelDataType operandType = argTypes.get(0);
        for (int i = 1; i < argTypes.size(); i++) {
            operandType = HazelcastTypeUtils.withHigherPrecedence(operandType, argTypes.get(i));
        }

        QueryDataType caseHzReturnType = HazelcastTypeUtils.toHazelcastType(operandType.getSqlTypeName());

        if (!allBranchTypesCanBeConvertedToReturnType(argTypes, operandList, operandType, caseHzReturnType)) {
            if (throwOnFailure) {
                throw QueryException.error(SqlErrorCode.GENERIC, "Cannot infer return type for COALESCE among " + argTypes);
            } else {
                return false;
            }
        }

        for (int index = 0; index < operandList.size(); index++) {
            validator.getTypeCoercion().coerceOperandType(scope, callBinding.getCall(), index, operandType);
        }

        return true;
    }


    private boolean allBranchTypesCanBeConvertedToReturnType(
            List<RelDataType> argTypes,
            List<SqlNode> allReturnNodes,
            RelDataType caseReturnType,
            QueryDataType caseHzReturnType) {
        for (int i = 0, argTypesSize = argTypes.size(); i < argTypesSize; i++) {
            RelDataType type = argTypes.get(i);
            QueryDataType hzType = HazelcastTypeUtils.toHazelcastType(type.getSqlTypeName());

            SqlNode sqlNode = allReturnNodes.get(i);

            if (!(hzType.getTypeFamily() == QueryDataTypeFamily.NULL
                    || type.equals(caseReturnType)
                    || bothParametersAreNumeric(caseHzReturnType, hzType)
                    || bothOperandsAreTemporalAndLowOperandCanBeConvertedToHighOperand(caseHzReturnType, hzType)
                    || highOperandIsTemporalAndLowOperandIsLiteralOfVarcharType(caseHzReturnType, hzType, sqlNode))) {
                return false;
            }
        }
        return true;
    }

    private static boolean bothParametersAreNumeric(QueryDataType highHZType, QueryDataType lowHZType) {
        return (highHZType.getTypeFamily().isNumeric() && lowHZType.getTypeFamily().isNumeric());
    }

    private static boolean bothOperandsAreTemporalAndLowOperandCanBeConvertedToHighOperand(QueryDataType highHZType,
                                                                                           QueryDataType lowHZType) {
        return highHZType.getTypeFamily().isTemporal()
                && lowHZType.getTypeFamily().isTemporal()
                && lowHZType.getConverter().canConvertTo(highHZType.getTypeFamily());
    }

    private static boolean highOperandIsTemporalAndLowOperandIsLiteralOfVarcharType(QueryDataType highHZType,
                                                                                    QueryDataType lowHZType, SqlNode low) {
        return highHZType.getTypeFamily().isTemporal()
                && lowHZType.getTypeFamily() == QueryDataTypeFamily.VARCHAR
                && low instanceof SqlLiteral;
    }
}
