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

import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;

public abstract class HazelcastWindowTableFunction extends HazelcastTableFunction {

    protected HazelcastWindowTableFunction(SqlKind kind, SqlOperandMetadata operandMetadata, int orderingColumnIndex) {
        super(kind.name(), operandMetadata, returnTypeInference(orderingColumnIndex));
    }

    private static SqlReturnTypeInference returnTypeInference(int orderingColumnIndex) {
        return binding -> {
            SqlCallBinding callBinding = ((SqlCallBinding) binding);
            SqlTypeName windowEdgeType = inferOrderingColumnType(callBinding, orderingColumnIndex);
            RelDataType inputRowType = callBinding.getValidator().getValidatedNodeType(callBinding.operand(0));
            return binding.getTypeFactory().builder()
                    .kind(inputRowType.getStructKind())
                    .addAll(inputRowType.getFieldList())
                    .add("window_start", windowEdgeType)
                    .add("window_end", windowEdgeType)
                    .build();
        };
    }

    private static SqlTypeName inferOrderingColumnType(SqlCallBinding binding, int orderingColumnIndex) {
        SqlNode input = binding.operand(0);

        SqlCall orderingColumn = (SqlCall) extractColumn(binding.operand(orderingColumnIndex));
        SqlIdentifier orderingColumnIdentifier = (SqlIdentifier) orderingColumn.getOperandList().get(0);
        String orderingColumnName = orderingColumnIdentifier.getSimple();

        SqlValidator validator = binding.getValidator();
        RelDataTypeField columnField = validator
                .getValidatedNodeType(input)
                .getField(orderingColumnName, validator.getCatalogReader().nameMatcher().isCaseSensitive(), false);
        return columnField.getType().getSqlTypeName();
    }

    private static SqlNode extractColumn(SqlNode operand) {
        return operand.getKind() == ARGUMENT_ASSIGNMENT ? ((SqlCall) operand).operand(0) : operand;
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }
}
