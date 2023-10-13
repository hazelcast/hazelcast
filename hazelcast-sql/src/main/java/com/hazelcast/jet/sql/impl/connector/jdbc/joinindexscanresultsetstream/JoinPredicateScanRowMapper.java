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

package com.hazelcast.jet.sql.impl.connector.jdbc.joinindexscanresultsetstream;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class iterates over the given leftRowsList and the ResultSet at the same time
 * During the iteration it generates JetSqlRow according to given SQL Join information.
 */
public class JoinPredicateScanRowMapper implements FunctionEx<ResultSet, JetSqlRow> {

    private final ExpressionEvalContext expressionEvalContext;

    private final JetJoinInfo joinInfo;

    private final List<Expression<?>> projections;

    private final List<JetSqlRow> leftRowsList;

    private Object[] values;

    private int queryNumberColumnIndex;

    private int leftRowIndex;

    private boolean moveResultSetForward = true;

    private boolean hasNext;

    private final Set<Integer> processedQueryNumbers = new HashSet<>();

    public JoinPredicateScanRowMapper(ExpressionEvalContext expressionEvalContext,
                                      List<Expression<?>> projections,
                                      JetJoinInfo joinInfo,
                                      List<JetSqlRow> leftRowsList) {
        this.expressionEvalContext = expressionEvalContext;
        this.projections = projections;
        this.joinInfo = joinInfo;
        this.leftRowsList = leftRowsList;
    }

    private ProcessingResult processResultSet(ResultSet resultSet) throws SQLException {
        ProcessingResult processingResult = new ProcessingResult();

        if (moveResultSetForward) {
            hasNext = resultSet.next();
        }
        if (hasNext) {
            fillValueArray(resultSet);
            int queryNumberFromResultSet = resultSet.getInt(queryNumberColumnIndex);
            // We have arrived to new query result
            if (leftRowIndex != queryNumberFromResultSet) {
                // No need to move the ResultSet forward
                moveResultSetForward = false;
                processingResult.jetSqlRow = processMismatchingQueryNumber();
                leftRowIndex++;
                processingResult.result = true;
            } else {
                // We are still at the same queryNumber
                moveResultSetForward = true;
                processingResult.jetSqlRow = processMatchingQueryNumber();
                processingResult.result = true;
            }
        }
        return processingResult;
    }

    @Override
    public JetSqlRow applyEx(ResultSet resultSet) throws SQLException {
        createValuesArrayIfNecessary(resultSet);

        // Start iterating over left rows
        while (leftRowIndex < leftRowsList.size()) {
            ProcessingResult processingResult = processResultSet(resultSet);
            if (processingResult.result) {
                if (processingResult.jetSqlRow != null) {
                    return processingResult.jetSqlRow;
                }
            } else {
                JetSqlRow jetSqlRow = processMismatchingQueryNumber();
                leftRowIndex++;
                if (jetSqlRow != null) {
                    return jetSqlRow;
                }
            }
        }
        return null;
    }

    private void createValuesArrayIfNecessary(ResultSet resultSet) throws SQLException {
        if (values == null) {
            values = createValueArrayExcludingQueryNumber(resultSet);
            queryNumberColumnIndex = getQueryNumberColumnIndex(resultSet);
        }
    }

    protected JetSqlRow createExtendedRowIfNecessary(JetSqlRow leftRow) {
        JetSqlRow result = null;
        if (!joinInfo.isInner()) {
            // This is not an inner join, so return a null padded JetSqlRow
            result = leftRow.extendedRow(projections.size());
        }
        return result;
    }

    protected Object[] createValueArrayExcludingQueryNumber(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        return new Object[columnCount - 1];
    }

    protected int getQueryNumberColumnIndex(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        return metaData.getColumnCount();
    }

    protected void fillValueArray(ResultSet resultSet) throws SQLException {
        for (int index = 0; index < values.length; index++) {
            // TODO we need to use mechanism similar (or maybe the same) to valueGetters in SelectProcessorSupplier
            values[index] = resultSet.getObject(index + 1);
        }
    }

    // Called when leftRowIndex is behind the query number or changing to a new query number
    private JetSqlRow processMismatchingQueryNumber() {
        JetSqlRow result = null;
        JetSqlRow leftRow = leftRowsList.get(leftRowIndex);
        // Check if we have processed this leftRow before
        if (!processedQueryNumbers.contains(leftRowIndex)) {
            result = createExtendedRowIfNecessary(leftRow);
            processedQueryNumbers.add(leftRowIndex);
        }
        return result;
    }

    private JetSqlRow processMatchingQueryNumber() {
        JetSqlRow result;
        JetSqlRow leftRow = leftRowsList.get(leftRowIndex);
        // Join the leftRow with the row from DB
        JetSqlRow jetSqlRowFromDB = new JetSqlRow(expressionEvalContext.getSerializationService(), values);
        JetSqlRow joinedRow = ExpressionUtil.join(leftRow, jetSqlRowFromDB, joinInfo.nonEquiCondition(),
                expressionEvalContext);
        if (joinedRow != null) {
            // The DB row evaluated as true
            result = joinedRow;
        } else {
            // The DB row evaluated as false
            result = createExtendedRowIfNecessary(leftRow);
        }
        processedQueryNumbers.add(leftRowIndex);
        return result;
    }
}
