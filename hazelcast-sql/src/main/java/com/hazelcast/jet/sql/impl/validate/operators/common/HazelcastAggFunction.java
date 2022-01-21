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

package com.hazelcast.jet.sql.impl.validate.operators.common;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

import static com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference.wrap;

/**
 * A common subclass for aggregate functions (in Jet).
 * <p>
 * See {@link HazelcastOperandTypeCheckerAware} for motivation.
 */
public abstract class HazelcastAggFunction extends SqlAggFunction implements HazelcastOperandTypeCheckerAware {

    public HazelcastAggFunction(
            String name,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory funcType,
            boolean requiresOrder,
            boolean requiresOver,
            Optionality requiresGroupOrder
    ) {
        super(name, null, kind, wrap(returnTypeInference), operandTypeInference, operandTypeChecker,
                funcType, requiresOrder, requiresOver, requiresGroupOrder);
    }

    @Override
    public final boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HazelcastCallBinding bindingOverride = prepareBinding(callBinding);

        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(1, 1);
    }

    protected abstract boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure);
}
