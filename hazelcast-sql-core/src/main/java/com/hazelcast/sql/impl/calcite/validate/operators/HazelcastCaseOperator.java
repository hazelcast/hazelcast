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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.common.AbstractCaseOperator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.Arrays;

public final class HazelcastCaseOperator extends AbstractCaseOperator {

    public static final HazelcastCaseOperator INSTANCE = new HazelcastCaseOperator();

    private HazelcastCaseOperator() {
        super();
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        // never used
        return true;
    }

    public static class CaseReturnTypeInference implements SqlReturnTypeInference {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding binding) {
            int size = binding.getOperandCount();
            RelDataType caseReturnType = binding.getOperandType(1);
            QueryDataType firstThenBranchType = HazelcastTypeUtils.toHazelcastType(caseReturnType.getSqlTypeName());
            QueryDataTypeFamily[] allReturnTypes = new QueryDataTypeFamily[size / 2 + 1];
            int j = 0;
            allReturnTypes[j] = firstThenBranchType.getTypeFamily();
            j++;
            boolean failure = false;
            for (int i = 1 + 2; i < size; i += 2) {
                QueryDataType operandType = HazelcastTypeUtils.toHazelcastType(binding.getOperandType(i).getSqlTypeName());
                failure |= !firstThenBranchType.equals(operandType);
                allReturnTypes[j++] = operandType.getTypeFamily();
            }
            QueryDataType elseType = HazelcastTypeUtils.toHazelcastType(binding.getOperandType(size - 1).getSqlTypeName());
            failure |= !firstThenBranchType.equals(elseType);
            allReturnTypes[j] = elseType.getTypeFamily();
            if (failure) {
                throw QueryException.error(
                        SqlErrorCode.GENERIC, "Cannot infer return type of case operator among " + Arrays.toString(allReturnTypes));
            }
            return caseReturnType;
        }
    }
}
