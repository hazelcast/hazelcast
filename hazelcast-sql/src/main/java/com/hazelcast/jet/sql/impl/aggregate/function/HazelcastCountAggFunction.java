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

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastAggFunction;
import com.hazelcast.jet.sql.impl.validate.param.NoOpParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastIntegerType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Optionality;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class HazelcastCountAggFunction extends HazelcastAggFunction {

    public HazelcastCountAggFunction() {
        super(
                "COUNT",
                SqlKind.COUNT,
                opBinding -> HazelcastIntegerType.create(Long.SIZE, false),
                new IgnoreCountStarOperandTypeInference(new ReplaceUnknownOperandTypeInference(BIGINT)),
                null,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION_STAR;
    }

    @Override
    public RelDataType deriveType(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlCall call
    ) {
        return HazelcastIntegerType.create(Long.SIZE, false);
    }

    protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        SqlNode node = binding.operand(0);
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            int parameterIndex = ((SqlDynamicParam) node).getIndex();
            binding.getValidator().setParameterConverter(parameterIndex, NoOpParameterConverter.INSTANCE);
        }

        // COUNT accepts any operand type
        return true;
    }

    /**
     * A wrapper for {@link SqlOperandTypeInference} that does nothing if the
     * call is COUNT(*). In other case it delegates to the wrapped inference.
     */
    private static class IgnoreCountStarOperandTypeInference implements SqlOperandTypeInference {
        private final SqlOperandTypeInference delegate;

        IgnoreCountStarOperandTypeInference(SqlOperandTypeInference delegate) {
            this.delegate = delegate;
        }

        @Override
        public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
            if (callBinding.getCall().isCountStar()) {
                return;
            }
            delegate.inferOperandTypes(callBinding, returnType, operandTypes);
        }
    }
}
