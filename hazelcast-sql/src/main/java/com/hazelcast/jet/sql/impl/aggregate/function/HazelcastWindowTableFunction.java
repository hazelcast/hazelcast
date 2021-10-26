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
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunction;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;
import static org.apache.calcite.sql.type.SqlTypeName.COLUMN_LIST;
import static org.apache.calcite.util.Static.RESOURCE;

public abstract class HazelcastWindowTableFunction extends HazelcastTableFunction {

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE = binding -> {
        SqlTypeName windowEdgeType = HazelcastTypeUtils.toCalciteType(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);
        SqlCallBinding callBinding = ((SqlCallBinding) binding);
        RelDataType inputRowType = callBinding.getValidator().getValidatedNodeType(callBinding.operand(0));
        return binding.getTypeFactory().builder()
                .kind(inputRowType.getStructKind())
                .addAll(inputRowType.getFieldList())
                .add("window_start", windowEdgeType, 3)
                .add("window_end", windowEdgeType, 3)
                .build();
    };

    protected HazelcastWindowTableFunction(SqlKind kind, SqlOperandMetadata operandMetadata) {
        super(kind.name(), operandMetadata, RETURN_TYPE_INFERENCE);
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    // TODO: ImposeOrderFunction.WindowOperandMetadata
    protected static final class WindowOperandMetadata extends HazelcastSqlOperandMetadata {

        public WindowOperandMetadata(List<HazelcastTableFunctionParameter> parameters) {
            super(parameters);
        }

        @Override
        protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
            HazelcastSqlValidator validator = binding.getValidator();
            SqlNode input = binding.operand(0);
            boolean result = binding.getCall().getOperandList().stream()
                    .filter(operand -> validator.deriveType(binding.getScope(), operand).getSqlTypeName() == COLUMN_LIST)
                    .map(operand -> operand.getKind() == ARGUMENT_ASSIGNMENT ? ((SqlCall) operand).operand(0) : operand)
                    .allMatch(operand -> checkTimeColumnDescriptorOperand(validator, input, (SqlCall) operand));

            if (!result && throwOnFailure) {
                throw binding.newValidationSignatureError();
            }
            return result;
        }

        private static boolean checkTimeColumnDescriptorOperand(
                SqlValidator validator,
                SqlNode input,
                SqlCall descriptor
        ) {
            List<SqlNode> descriptorIdentifiers = descriptor.getOperandList();
            if (descriptorIdentifiers.size() != 1) {
                return false;
            }

            SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();

            SqlIdentifier timeColumnIdentifier = (SqlIdentifier) descriptorIdentifiers.get(0);
            String timeColumnName = timeColumnIdentifier.getSimple();
            RelDataTypeField timeColumnField = validator.getValidatedNodeType(input).getFieldList().stream()
                    .filter(field -> matcher.matches(field.getName(), timeColumnName))
                    .findFirst()
                    .orElseThrow(() -> SqlUtil.newContextException(
                            timeColumnIdentifier.getParserPosition(),
                            RESOURCE.unknownIdentifier(timeColumnName)
                    ));
            return timeColumnField.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        }
    }
}
