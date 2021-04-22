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
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastAggFunction;
import com.hazelcast.sql.impl.calcite.validate.param.NoOpParameterConverter;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class HazelcastMinMaxAggFunction extends HazelcastAggFunction {

    public HazelcastMinMaxAggFunction(SqlKind kind) {
        super(
                kind.name(),
                kind,
                ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                new ReplaceUnknownOperandTypeInference(BIGINT),
                null,
                SqlFunctionCategory.SYSTEM,
                false,
                false,
                Optionality.FORBIDDEN);
    }

    protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        SqlNode node = binding.operand(0);
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            int parameterIndex = ((SqlDynamicParam) node).getIndex();
            binding.getValidator().setParameterConverter(parameterIndex, NoOpParameterConverter.INSTANCE);
        }

        // MIN/MAX accepts any operand type (though it must be Comparable at runtime)
        return true;
    }
}
