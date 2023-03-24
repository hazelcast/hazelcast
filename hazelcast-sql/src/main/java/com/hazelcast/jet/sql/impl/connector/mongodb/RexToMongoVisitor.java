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

package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.impl.expression.Expression;
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
import org.apache.calcite.util.NlsString;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * Visitor that converts REX nodes to Mongo expressions.
 */
final class RexToMongoVisitor implements RexVisitor<Object> {

    private static final Object[] EMPTY_EXPRESSION_OPERANDS = new Object[0];

    @Override
    public Object visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        return new InputRef(index).asString();
    }

    @Override
    public Expression<?> visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object visitLiteral(RexLiteral literal) {
        Comparable value = literal.getValue();
        if (value instanceof NlsString) {
            return ((NlsString) value).getValue();
        }
        return value;
    }

    @Override
    public Bson visitCall(RexCall call) {
        // Convert the operands.
        List<RexNode> rexOperands = call.getOperands();
        Object[] operands;

        if (rexOperands == null || rexOperands.isEmpty()) {
            operands = EMPTY_EXPRESSION_OPERANDS;
        } else {
            operands = new Object[rexOperands.size()];
            for (int i = 0; i < rexOperands.size(); ++i) {
                Object expressionOperand = rexOperands.get(i).accept(this);
                operands[i] = expressionOperand;
            }
        }

        // Convert the call.
        return RexToMongo.convertCall(call, operands);
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
    public Object visitDynamicParam(RexDynamicParam dynamicParam) {
        int index = dynamicParam.getIndex();
        return new DynamicParameter(index).asString();
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
