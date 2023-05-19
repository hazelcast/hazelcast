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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.mongodb.client.model.Filters;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;

/**
 * Utility methods for REX to Mongo expression conversion.
 */
public final class RexToMongo {

    private RexToMongo() {
        // No-op.
    }

    /**
     * Converts a {@link RexCall} to {@link Expression}.
     *
     * @param call the call to convert.
     * @return the resulting expression.
     * @throws QueryException if the given {@link RexCall} can't be
     *                        converted.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:ReturnCount",
            "checkstyle:NPathComplexity", "checkstyle:MagicNumber"})
    public static Bson convertCall(RexCall call, Object[] operands) {
        SqlOperator operator = call.getOperator();

        switch (operator.getKind()) {
            case AND:
                return Filters.and(convertOperands(operands));

            case OR:
                return Filters.or(convertOperands(operands));

            case NOT:
                if (operands[0] instanceof String) {
                    // simple column, meaning it's boolean
                    assert call.getOperands().get(0).getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN);
                    return Filters.ne((String) operands[0], true);
                } else {
                    return Filters.not((Bson) operands[0]);
                }

            case EQUALS:
                assert operands[0] instanceof String;
                return Filters.expr(new Document("$eq", Arrays.asList(operands[0], operands[1])));

            case NOT_EQUALS:
                assert operands[0] instanceof String;
                return Filters.expr(new Document("$ne", Arrays.asList(operands[0], operands[1])));

            case GREATER_THAN:
                assert operands[0] instanceof String;
                return Filters.expr(new Document("$gt", Arrays.asList(operands[0], operands[1])));

            case GREATER_THAN_OR_EQUAL:
                assert operands[0] instanceof String;
                return Filters.expr(new Document("$gte", Arrays.asList(operands[0], operands[1])));

            case LESS_THAN:
                assert operands[0] instanceof String;
                return Filters.expr(new Document("$lt", Arrays.asList(operands[0], operands[1])));

            case LESS_THAN_OR_EQUAL:
                assert operands[0] instanceof String;
                return Filters.expr(new Document("$lte", Arrays.asList(operands[0], operands[1])));

            case IS_TRUE:
                return Filters.eq((String) operands[0], true);

            case IS_NOT_TRUE:
                return Filters.ne((String) operands[0], true);

            case IS_FALSE:
                return Filters.eq((String) operands[0], false);

            case IS_NOT_FALSE:
                return Filters.ne((String) operands[0], false);
            case IS_NULL:
                return Filters.or(
                        Filters.exists((String) operands[0], false),
                        Filters.eq((String) operands[0], null)
                );
            case IS_NOT_NULL:
                return Filters.and(
                        Filters.exists((String) operands[0]),
                        Filters.ne((String) operands[0], null)
                );
            default:
                throw new UnsupportedOperationException();
        }

    }

    private static Bson[] convertOperands(Object[] operands) {
        Bson[] r = new Bson[operands.length];
        for (int i = 0; i < operands.length; i++) {
            Object ith = operands[i];
            if (ith instanceof String) {
                r[i] = Filters.eq((String) ith, true);
            } else if (ith instanceof Bson) {
                r[i] = (Bson) ith;
            } else {
                throw new UnsupportedOperationException("Not supported type of operand: " + ith.getClass());
            }
        }
        return r;
    }
}
