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

package com.hazelcast.jet.sql.impl.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandChecker;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;

import static com.hazelcast.jet.sql.impl.validate.ValidatorResource.RESOURCE;

public class NamedOperandCheckerProgram {

    private final OperandChecker[] checkers;

    public NamedOperandCheckerProgram(OperandChecker... checkers) {
        this.checkers = checkers;
    }

    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        boolean res = true;

        SqlCall call = callBinding.getCall();
        SqlFunction operator = (SqlFunction) call.getOperator();
        for (int i = 0; i < call.operandCount(); i++) {
            SqlNode operand = call.operand(i);
            assert operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT;

            SqlIdentifier id = ((SqlCall) operand).operand(1);
            OperandChecker checker = findOperandChecker(id, operator);

            res &= checker.check(callBinding, false, i);
        }

        if (!res && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return res;
    }

    private OperandChecker findOperandChecker(SqlIdentifier identifier, SqlFunction operator) {
        String name = identifier.getSimple();
        for (int i = 0; i < operator.getParamNames().size(); i++) {
            String paramName = operator.getParamNames().get(i);
            if (paramName.equals(name)) {
                return checkers[i];
            }
        }
        throw SqlUtil.newContextException(identifier.getParserPosition(), RESOURCE.unknownArgumentName(name));
    }
}
