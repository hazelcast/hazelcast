/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class Filters {

    private static final Set<SqlKind> SUPPORTED_COMPARISONS =
            EnumSet.of(SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.LESS_THAN, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.LESS_THAN_OR_EQUAL);

    private static final Set<SqlTypeName> SUPPORTED_TYPES =
            EnumSet.of(SqlTypeName.BOOLEAN, SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.CHAR, SqlTypeName.VARCHAR,
                    SqlTypeName.DECIMAL);

    private static final Set<SqlKind> SUPPORTED_LOGIC = EnumSet.of(SqlKind.AND, SqlKind.OR, SqlKind.NOT);

    public static boolean isSupported(Filter filter) {
        return isSupported(filter.getCondition());
    }

    public static Predicate convert(Filter filter) {
        return convert(filter.getCondition(), filter);
    }

    private Filters() {
    }

    private static boolean isSupported(RexNode node) {
        if (isField(node)) {
            // TODO: make sure it's a boolean field ref
            return true;
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            if (call.op.kind.belongsTo(SUPPORTED_COMPARISONS)) {
                List<RexNode> operands = call.getOperands();
                RexLiteral literal;
                if (isField(operands.get(0)) && operands.get(1) instanceof RexLiteral) {
                    literal = (RexLiteral) operands.get(1);
                } else if (operands.get(0) instanceof RexLiteral && isField(operands.get(1))) {
                    literal = (RexLiteral) operands.get(0);
                } else {
                    return false;
                }

                return SUPPORTED_TYPES.contains(literal.getType().getSqlTypeName());
            }

            if (call.op.kind.belongsTo(SUPPORTED_LOGIC)) {
                for (RexNode operand : call.getOperands()) {
                    if (!isSupported(operand)) {
                        return false;
                    }
                }

                return true;
            }
        }

        return false;
    }

    private static boolean isField(RexNode node) {
        // TODO: validate ref is really a table field
        if (node instanceof RexInputRef) {
            return true;
        }

        // TODO: proper cast support
        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if (rexCall.op.kind == SqlKind.CAST) {
                assert rexCall.operands.size() == 1;
                return isField(rexCall.operands.get(0));
            }
        }

        return false;
    }

    private static Predicate convert(RexNode node, Filter filter) {
        if (isField(node)) {
            // TODO: make sure it's a boolean field ref
            // TODO: proper field name resolution

            RexInputRef field = resolveField(node);
            String attribute = filter.getRowType().getFieldList().get(field.getIndex()).getName();
            return Predicates.equal(attribute, true);
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            if (call.op.kind.belongsTo(SUPPORTED_COMPARISONS)) {
                List<RexNode> operands = call.getOperands();
                RexInputRef field;
                RexLiteral literal;
                if (isField(operands.get(0)) && operands.get(1) instanceof RexLiteral) {
                    field = resolveField(operands.get(0));
                    literal = (RexLiteral) operands.get(1);
                } else if (operands.get(0) instanceof RexLiteral && isField(operands.get(1))) {
                    field = resolveField(operands.get(1));
                    literal = (RexLiteral) operands.get(0);
                } else {
                    throw new IllegalStateException();
                }

                // TODO: proper field name resolution
                String attribute = filter.getRowType().getFieldList().get(field.getIndex()).getName();
                Comparable value = literal.getValue();

                // TODO: Calcite represents decimal literals as BigDecimal, but fails to generated the proper code
                if (value instanceof BigDecimal) {
                    value = value.toString();
                }

                switch (call.op.kind) {
                    case EQUALS:
                        return Predicates.equal(attribute, value);
                    case NOT_EQUALS:
                        return Predicates.notEqual(attribute, value);
                    case LESS_THAN:
                        return Predicates.lessThan(attribute, value);
                    case GREATER_THAN:
                        return Predicates.greaterThan(attribute, value);
                    case LESS_THAN_OR_EQUAL:
                        return Predicates.lessEqual(attribute, value);
                    case GREATER_THAN_OR_EQUAL:
                        return Predicates.greaterEqual(attribute, value);
                }
            }

            if (call.op.kind.belongsTo(SUPPORTED_LOGIC)) {
                List<RexNode> operands = call.getOperands();

                switch (call.op.kind) {
                    case AND:
                        Predicate[] andPredicates = new Predicate[operands.size()];
                        for (int i = 0; i < operands.size(); ++i) {
                            andPredicates[i] = convert(operands.get(i), filter);
                        }
                        return Predicates.and(andPredicates);
                    case OR:
                        Predicate[] orPredicates = new Predicate[operands.size()];
                        for (int i = 0; i < operands.size(); ++i) {
                            orPredicates[i] = convert(operands.get(i), filter);
                        }
                        return Predicates.or(orPredicates);
                    case NOT:
                        assert operands.size() == 1;
                        return Predicates.not(convert(operands.get(0), filter));
                }
            }

        }

        throw new IllegalStateException();
    }

    private static RexInputRef resolveField(RexNode node) {
        if (node instanceof RexInputRef) {
            return (RexInputRef) node;
        }

        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if (rexCall.op.kind == SqlKind.CAST) {
                assert rexCall.operands.size() == 1;
                return resolveField(rexCall.operands.get(0));
            }
        }

        throw new IllegalStateException();
    }

}
