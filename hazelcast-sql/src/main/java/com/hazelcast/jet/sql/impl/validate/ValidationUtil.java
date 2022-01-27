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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import static com.hazelcast.jet.sql.impl.schema.TableResolverImpl.SCHEMA_NAME_PUBLIC;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;
import static org.apache.calcite.sql.SqlKind.AS;

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

    /**
     * If the operand is an AS call, returns its target. Otherwise
     * return the operand.
     */
    public static RexNode unwrapAsOperatorOperand(RexNode operand) {
        return operand.getKind() == AS ? ((RexCall) operand).getOperands().get(0) : operand;
    }

    /**
     * Returns true if the view name is in a valid schema,
     * that is it must be either:
     * <ul>
     *     <li>a simple name
     *     <li>a name in schema "public"
     *     <li>a name in schema "hazelcast.public"
     * </ul>
     */
    @SuppressWarnings({"checkstyle:BooleanExpressionComplexity", "BooleanMethodIsAlwaysInverted"})
    public static boolean isCatalogObjectNameValid(SqlIdentifier name) {
        return name.names.size() == 1
                || name.names.size() == 2 && SCHEMA_NAME_PUBLIC.equals(name.names.get(0))
                || name.names.size() == 3 && CATALOG.equals(name.names.get(0))
                && SCHEMA_NAME_PUBLIC.equals(name.names.get(1));
    }
}
