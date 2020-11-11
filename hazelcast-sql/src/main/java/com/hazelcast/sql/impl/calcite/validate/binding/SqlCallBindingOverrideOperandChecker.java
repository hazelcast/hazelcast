/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.binding;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

public class SqlCallBindingOverrideOperandChecker implements SqlOperandTypeChecker {

    private final SqlOperandTypeChecker delegate;

    public SqlCallBindingOverrideOperandChecker(SqlOperandTypeChecker delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        SqlCallBinding callBindingOverride = new SqlCallBindingOverride(callBinding);

        return delegate.checkOperandTypes(callBindingOverride, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return delegate.getOperandCountRange();
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return delegate.getAllowedSignatures(op, opName);
    }

    @Override
    public Consistency getConsistency() {
        return delegate.getConsistency();
    }

    @Override
    public boolean isOptional(int i) {
        return delegate.isOptional(i);
    }
}
