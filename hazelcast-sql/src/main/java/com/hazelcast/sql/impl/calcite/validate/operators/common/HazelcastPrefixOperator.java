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

package com.hazelcast.sql.impl.calcite.validate.operators.common;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;

/**
 * A common subclass for prefix operators.
 * <p>
 * See {@link HazelcastOperandTypeCheckerAware} for motivation.
 */
public abstract class HazelcastPrefixOperator extends SqlPrefixOperator implements HazelcastOperandTypeCheckerAware {
    protected HazelcastPrefixOperator(
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
