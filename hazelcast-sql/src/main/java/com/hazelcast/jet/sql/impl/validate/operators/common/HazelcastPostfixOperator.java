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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference.wrap;

/**
 * A common subclass for postfix operators.
 * <p>
 * See {@link HazelcastOperandTypeCheckerAware} for motivation.
 */
public abstract class HazelcastPostfixOperator extends SqlPostfixOperator implements HazelcastOperandTypeCheckerAware {
    protected HazelcastPostfixOperator(
            String name,
            SqlKind kind,
            int prec,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference
    ) {
        super(name, kind, prec, wrap(returnTypeInference), operandTypeInference, null);
    }

    @Override
    public final boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HazelcastCallBinding bindingOverride = prepareBinding(callBinding);

        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    protected abstract boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure);
}
