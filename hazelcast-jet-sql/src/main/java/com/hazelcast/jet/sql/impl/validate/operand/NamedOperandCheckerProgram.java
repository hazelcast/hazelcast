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

package com.hazelcast.jet.sql.impl.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandChecker;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

public class NamedOperandCheckerProgram {

    private final OperandChecker[] checkers;

    public NamedOperandCheckerProgram(OperandChecker... checkers) {
        this.checkers = checkers;
    }

    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        boolean res = true;

        SqlFunction operator = (SqlFunction) callBinding.getCall().getOperator();
        for (int i = 0; i < operator.getParamNames().size(); i++) {
            String paramName = operator.getParamNames().get(i);

            int index = findOperandIndex(callBinding, paramName);
            if (index == -1) {
                // delegate missing arguments validation to the operator
                continue;
            }

            res &= checkers[i].check(callBinding, false, index);
        }

        if (!res && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return res;
    }

    private int findOperandIndex(HazelcastCallBinding callBinding, String paramName) {
        SqlCall call = callBinding.getCall();

        for (int i = 0; i < call.getOperandList().size(); i++) {
            SqlNode operand = call.getOperandList().get(i);
            assert operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT;

            SqlIdentifier id = ((SqlCall) operand).operand(1);
            if (id.getSimple().equals(paramName)) {
                return i;
            }
        }

        return -1;
    }
}
