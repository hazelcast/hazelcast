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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Composite program that invokes a separate operand checker for every operand.
 */
public class OperandCheckerProgram {

    private final OperandChecker[] checkers;

    public OperandCheckerProgram(OperandChecker... checkers) {
        this.checkers = checkers;
    }

    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        boolean res = true;

        boolean hasAssignment = hasAssignment(callBinding.getCall());
        for (int i = 0; i < checkers.length; i++) {
            OperandChecker checker = hasAssignment ? findCheckerByParamName(callBinding, i) : checkers[i];

            boolean checkerRes = checker.check(callBinding, false, i);
            if (!checkerRes) {
                res = false;
            }
        }

        if (!res && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return res;
    }

    private boolean hasAssignment(SqlCall call) {
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null && operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                return true;
            }
        }
        return false;
    }

    private OperandChecker findCheckerByParamName(SqlCallBinding binding, int index) {
        SqlCall call = binding.getCall();
        SqlFunction operator = (SqlFunction) call.getOperator();

        String paramName = operator.getParamNames().get(index);
        for (int i = 0; i < call.getOperandList().size(); i++) {
            SqlNode operand = call.getOperandList().get(i);
            assert operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT;

            SqlIdentifier id = ((SqlCall) operand).operand(1);
            if (id.getSimple().equals(paramName)) {
                return checkers[i];
            }
        }

        throw binding.newValidationError(RESOURCE.defaultForOptionalParameter());
    }
}
