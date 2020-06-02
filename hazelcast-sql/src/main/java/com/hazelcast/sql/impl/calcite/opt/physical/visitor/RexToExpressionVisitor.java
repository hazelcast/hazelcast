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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

import java.util.List;

/**
 * Visitor that converts REX nodes to Hazelcast expressions.
 */
public final class RexToExpressionVisitor implements RexVisitor<Expression<?>> {

    private static final Expression<?>[] EMPTY_EXPRESSION_OPERANDS = new Expression[0];

    private final PlanNodeFieldTypeProvider fieldTypeProvider;

    public RexToExpressionVisitor(PlanNodeFieldTypeProvider fieldTypeProvider) {
        this.fieldTypeProvider = fieldTypeProvider;
    }

    @Override
    public Expression<?> visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        return ColumnExpression.create(index, fieldTypeProvider.getType(index));
    }

    @Override
    public Expression<?> visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitLiteral(RexLiteral literal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitCall(RexCall call) {
        // Convert the operands.
        List<RexNode> rexOperands = call.getOperands();
        Expression<?>[] expressionOperands;

        if (rexOperands == null || rexOperands.isEmpty()) {
            expressionOperands = EMPTY_EXPRESSION_OPERANDS;
        } else {
            expressionOperands = new Expression[rexOperands.size()];
            for (int i = 0; i < rexOperands.size(); ++i) {
                Expression<?> expressionOperand = rexOperands.get(i).accept(this);
                expressionOperands[i] = expressionOperand;
            }
        }

        // Convert the call.
        return RexToExpression.convertCall(call, expressionOperands);
    }

    @Override
    public Expression<?> visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression<?> visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException();
    }
}
