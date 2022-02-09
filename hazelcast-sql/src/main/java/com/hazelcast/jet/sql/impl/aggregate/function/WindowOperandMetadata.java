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

import com.hazelcast.jet.sql.impl.schema.HazelcastSqlOperandMetadata;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

import static com.hazelcast.jet.sql.impl.aggregate.WindowUtils.getOrderingColumnType;
import static com.hazelcast.jet.sql.impl.validate.HazelcastResources.RESOURCES;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName;

final class WindowOperandMetadata extends HazelcastSqlOperandMetadata {

    private final int[] operandIndexes;

    /**
     * @param operandIndexes Indexes of operands that contain INTERVAL values that need to match
     *                       the timestamp column type.
     */
    WindowOperandMetadata(List<HazelcastTableFunctionParameter> parameters, int[] operandIndexes) {
        super(parameters);
        this.operandIndexes = operandIndexes;
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        boolean result = true;
        for (int columnIndex : operandIndexes) {
            result &= checkOperandTypes(binding, throwOnFailure, columnIndex);
        }
        assert result || !throwOnFailure;
        return result;
    }

    private boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure, int columnIndex) {
        SqlNode lag = binding.operand(columnIndex);
        HazelcastSqlValidator validator = binding.getValidator();
        SqlTypeName orderingColumnType = getOrderingColumnType(binding, 1).getSqlTypeName();
        boolean result;
        SqlTypeName lagType = ((SqlValidator) validator).getValidatedNodeType(lag).getSqlTypeName();
        if (SqlTypeName.INT_TYPES.contains(orderingColumnType)) {
            result = SqlTypeName.INT_TYPES.contains(lagType);
        } else if (SqlTypeName.DATETIME_TYPES.contains(orderingColumnType)) {
            result = lagType.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME;
        } else {
            result = false;
        }

        if (!result && throwOnFailure) {
            QueryDataTypeFamily hzOrderingColumnType = toHazelcastTypeFromSqlTypeName(orderingColumnType).getTypeFamily();
            QueryDataTypeFamily hzLagType = toHazelcastTypeFromSqlTypeName(lagType).getTypeFamily();
            throw validator.newValidationError(binding.getCall(),
                    RESOURCES.windowFunctionTypeMismatch(hzOrderingColumnType.toString(), hzLagType.toString()));
        }
        return result;
    }
}
