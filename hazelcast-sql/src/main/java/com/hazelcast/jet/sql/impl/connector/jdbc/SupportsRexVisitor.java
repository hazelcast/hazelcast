/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.Set;

public class SupportsRexVisitor extends RexVisitorImpl<Boolean> {

    private static final Set<String> OTHER_SUPPORTED = ImmutableSet.of(
            "||",
            "NOT LIKE",
            "LENGTH",
            "LOWER",
            "UPPER"
    );

    protected SupportsRexVisitor() {
        super(true);
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
        return true;
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
        return true;
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
        return true;
    }

    @Override
    public Boolean visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        SqlKind kind = operator.getKind();

        return operatorSupported(operator, kind) && operandsSupported(call);
    }

    private static boolean operatorSupported(SqlOperator operator, SqlKind kind) {
        switch (kind) {
            case EQUALS:
            case AND:
            case OR:
            case NOT:
            case NOT_EQUALS:
            case SEARCH:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LIKE:
            case IS_NULL:
            case IS_NOT_NULL:
            case NULLIF:
            case IS_NOT_TRUE:
            case IS_NOT_FALSE:
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case COALESCE:
            case CAST:
                return true;

            case OTHER:
            case OTHER_FUNCTION:
                return OTHER_SUPPORTED.contains(operator.getName());

            default:
                return false;
        }
    }

    private boolean operandsSupported(RexCall call) {
        for (RexNode operand : call.operands) {
            Boolean supports = operand.accept(this);
            if (supports == null || !supports) {
                return false;
            }
        }
        return true;
    }
}
