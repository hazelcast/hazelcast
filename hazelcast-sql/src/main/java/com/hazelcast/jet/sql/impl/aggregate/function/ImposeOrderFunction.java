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
import com.hazelcast.jet.sql.impl.validate.ValidatorResource;
import com.hazelcast.jet.sql.impl.validate.operand.AnyOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.DescriptorOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.RowOperandChecker;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;
import static org.apache.calcite.sql.type.SqlTypeName.COLUMN_LIST;
import static org.apache.calcite.util.Static.RESOURCE;

public class ImposeOrderFunction extends HazelcastTableFunction {

    private static final List<HazelcastTableFunctionParameter> PARAMETERS = asList(
            new HazelcastTableFunctionParameter(0, "input", SqlTypeName.ROW, false, RowOperandChecker.INSTANCE),
            new HazelcastTableFunctionParameter(1, "column", SqlTypeName.COLUMN_LIST, false, DescriptorOperandChecker.INSTANCE),
            new HazelcastTableFunctionParameter(2, "lag", SqlTypeName.ANY, false, AnyOperandChecker.INSTANCE)
    );

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE = binding -> {
        SqlCallBinding callBinding = ((SqlCallBinding) binding);
        return callBinding.getValidator().getValidatedNodeType(callBinding.operand(0));
    };

    public ImposeOrderFunction() {
        super("IMPOSE_ORDER", new WindowOperandMetadata(PARAMETERS), RETURN_TYPE_INFERENCE);
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    private static final class WindowOperandMetadata extends HazelcastSqlOperandMetadata {

        private WindowOperandMetadata(List<HazelcastTableFunctionParameter> parameters) {
            super(parameters);
        }

        @Override
        protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
            HazelcastSqlValidator validator = binding.getValidator();
            SqlNode input = binding.operand(0);
            SqlNode lag = binding.operand(2);
            boolean result = binding.getCall().getOperandList().stream()
                    .filter(operand -> validator.deriveType(binding.getScope(), operand).getSqlTypeName() == COLUMN_LIST)
                    .map(operand -> operand.getKind() == ARGUMENT_ASSIGNMENT ? ((SqlCall) operand).operand(0) : operand)
                    .allMatch(operand -> checkColumnOperand(validator, input, (SqlCall) operand, lag));

            if (!result && throwOnFailure) {
                throw binding.newValidationSignatureError();
            }
            return result;
        }

        private static boolean checkColumnOperand(SqlValidator validator, SqlNode input, SqlCall descriptor, SqlNode lag) {
            SqlIdentifier descriptorIdentifier = checkDescriptorCardinality(descriptor);
            RelDataTypeField columnField = checkColumnName(validator, input, descriptorIdentifier);
            return checkColumnType(validator, columnField, lag);
        }

        private static SqlIdentifier checkDescriptorCardinality(SqlCall descriptor) {
            List<SqlNode> descriptorIdentifiers = descriptor.getOperandList();
            if (descriptorIdentifiers.size() != 1) {
                throw SqlUtil.newContextException(
                        descriptor.getParserPosition(),
                        ValidatorResource.RESOURCE.mustUseSingleOrderingColumn()
                );
            }
            return (SqlIdentifier) descriptorIdentifiers.get(0);
        }

        private static RelDataTypeField checkColumnName(
                SqlValidator validator,
                SqlNode input,
                SqlIdentifier descriptorIdentifier
        ) {
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
}
