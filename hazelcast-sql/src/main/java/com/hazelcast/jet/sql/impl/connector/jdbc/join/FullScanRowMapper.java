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

package com.hazelcast.jet.sql.impl.connector.jdbc.join;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector;
import com.hazelcast.jet.sql.impl.connector.jdbc.TypeResolver;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

public class FullScanRowMapper implements Function<ResultSet, JetSqlRow> {

    private final ExpressionEvalContext expressionEvalContext;

    private final TypeResolver typeResolver;
    private final List<FunctionEx<Object, ?>> converters;

    private final JetJoinInfo joinInfo;

    private final List<Expression<?>> projections;
    private final JetSqlRow leftRow;

    private Object[] values;
    private BiFunctionEx<ResultSet, Integer, ?>[] valueGetters;

    public FullScanRowMapper(ExpressionEvalContext expressionEvalContext,
                             TypeResolver typeResolver,
                             List<FunctionEx<Object, ?>> converters,
                             List<Expression<?>> projections,
                             JetJoinInfo joinInfo,
                             JetSqlRow leftRow) {
        this.expressionEvalContext = expressionEvalContext;
        this.typeResolver = typeResolver;
        this.converters = converters;
        this.projections = projections;
        this.joinInfo = joinInfo;
        this.leftRow = leftRow;
    }

    @Override
    public JetSqlRow apply(ResultSet resultSet) {
        try {
            if (values == null) {
                values = createValueArray(resultSet);
            }
            fillValueArray(resultSet, values);

            JetSqlRow jetSqlRowFromDB = new JetSqlRow(
                    expressionEvalContext.getSerializationService(),
                    values);

            // Join the leftRow with the row from DB
            JetSqlRow joinedRow = ExpressionUtil.join(leftRow,
                    jetSqlRowFromDB,
                    joinInfo.nonEquiCondition(),
                    expressionEvalContext);

            if (joinedRow != null) {
                // The DB row evaluated as true
                return joinedRow;
            } else {
                // The DB row evaluated as false
                if (!joinInfo.isInner()) {
                    // This is not an inner join, so return a null padded JetSqlRow
                    return createExtendedRow(leftRow);
                }
            }
            return null;
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    protected static Object[] createValueArray(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        return new Object[columnCount];
    }

    protected void fillValueArray(ResultSet resultSet, Object[] values) throws SQLException {
        if (valueGetters == null) {
            valueGetters = JdbcSqlConnector.prepareValueGettersFromMetadata(typeResolver, resultSet, converters::get);
        }

        for (int index = 0; index < values.length; index++) {
            values[index] = valueGetters[index].apply(resultSet, index + 1);
        }
    }
    protected JetSqlRow createExtendedRow(JetSqlRow leftRow) {
        return leftRow.extendedRow(projections.size());
    }
}
