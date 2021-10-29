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

package com.hazelcast.jet.sql.impl.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;

public final class ValidationUtil {

    private ValidationUtil() {
    }

    public static boolean hasAssignment(SqlCall call) {
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null
                    && operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                return true;
            }
        }
        return false;
    }

    /**
     * If the operand is an ARGUMENT_ASSIGNMENT, returns its target.
     * Otherwise return the operand.
     */
    public static SqlNode unwrapFunctionOperand(SqlNode operand) {
        return operand.getKind() == ARGUMENT_ASSIGNMENT ? ((SqlCall) operand).operand(0) : operand;
    }
}
