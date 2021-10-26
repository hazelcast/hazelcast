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
import com.hazelcast.jet.sql.impl.validate.ValidatorResource;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;
import static org.apache.calcite.util.Static.RESOURCE;

public final class WindowOperandMetadata extends HazelcastSqlOperandMetadata {

    WindowOperandMetadata(List<HazelcastTableFunctionParameter> parameters) {
        super(parameters);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        HazelcastSqlValidator validator = binding.getValidator();
        SqlNode input = binding.operand(0);
        SqlNode column = extractColumn(binding.operand(1));
        SqlNode lag = binding.operand(2);
        boolean result = checkColumnOperand(validator, input, (SqlCall) column, lag);

        if (!result && throwOnFailure) {
            throw binding.newValidationSignatureError();
        }
        return result;
    }

    private static SqlNode extractColumn(SqlNode operand) {
        return operand.getKind() == ARGUMENT_ASSIGNMENT ? ((SqlCall) operand).operand(0) : operand;
    }

    private static boolean checkColumnOperand(SqlValidator validator, SqlNode input, SqlCall column, SqlNode lag) {
        SqlIdentifier columnIdentifier = checkDescriptorCardinality(column);
        RelDataTypeField columnField = checkColumnName(validator, input, columnIdentifier);
        return checkColumnType(validator, columnField, lag);
    }

    private static SqlIdentifier checkDescriptorCardinality(SqlCall descriptor) {
        List<SqlNode> columnIdentifiers = descriptor.getOperandList();
        if (columnIdentifiers.size() != 1) {
            throw SqlUtil.newContextException(
                    descriptor.getParserPosition(),
                    ValidatorResource.RESOURCE.mustUseSingleOrderingColumn()
            );
        }
        return (SqlIdentifier) columnIdentifiers.get(0);
    }

    /**
     * @return The field from {@code input} referenced by the {@code
     * descriptorIdentifier}
     */
    private static RelDataTypeField checkColumnName(SqlValidator validator, SqlNode input, SqlIdentifier descriptorIdentifier) {
        SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
        String columnName = descriptorIdentifier.getSimple();
        return validator.getValidatedNodeType(input).getFieldList().stream()
                .filter(field -> matcher.matches(field.getName(), columnName))
                .findFirst()
                .orElseThrow(() -> SqlUtil.newContextException(
                        descriptorIdentifier.getParserPosition(),
                        RESOURCE.unknownIdentifier(columnName)
                ));
    }

    private static boolean checkColumnType(SqlValidator validator, RelDataTypeField columnField, SqlNode lag) {
        SqlTypeName timeColumnType = columnField.getType().getSqlTypeName();
        SqlTypeName lagType = validator.getValidatedNodeType(lag).getSqlTypeName();
        if (SqlTypeName.INT_TYPES.contains(timeColumnType)) {
            return SqlTypeName.INT_TYPES.contains(lagType);
        } else if (SqlTypeName.DATETIME_TYPES.contains(timeColumnType)) {
            return lagType.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME;
        } else {
            return false;
        }
    }
}
