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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.lang.NonNull;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class JdbcJoinIndexScanProcessorSupplier
        extends AbstractJoinProcessorSupplier
        implements DataSerializable, SecuredFunction {

    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    @SuppressWarnings("unused")
    public JdbcJoinIndexScanProcessorSupplier() {
    }

    public JdbcJoinIndexScanProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String query,
            @NonNull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName, query, joinInfo, projections);
    }

    protected Traverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows) {
        List<JetSqlRow> leftRowsList = convertIterableToArrayList(leftRows);
        String unionAllSql = generateSql(leftRowsList);

        List<JetSqlRow> resultRows = joinUnionAll(leftRowsList, unionAllSql);

        return traverseIterable(resultRows);
    }

    private <T> ArrayList<T> convertIterableToArrayList(Iterable<T> iterable) {
        Stream<T> stream = StreamSupport.stream(iterable.spliterator(), false);
        return stream.collect(Collectors.toCollection(ArrayList::new));
    }

    private String generateSql(List<JetSqlRow> leftRowsList) {
        String delimiter = "UNION ALL ";
        return IntStream.range(0, leftRowsList.size())
                        .mapToObj(i -> query.replaceFirst(IndexScanSelectQueryBuilder.ROW_NUMBER_LITERAL,
                                String.valueOf(i)))
                        .collect(Collectors.joining(delimiter));
    }

    private List<JetSqlRow> joinUnionAll(List<JetSqlRow> leftRowsList, String unionAllSql) {
        List<JetSqlRow> resultRows = new ArrayList<>();

        // Index scan : Set the parameters to PreparedStatement and iterate over the ResulSet
        try (Connection connection = dataConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(unionAllSql)) {

            setObjectsToPreparedStatement(preparedStatement, leftRowsList);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {

                Object[] values = createValueArrayExcludingQueryNumber(resultSet);
                int queryNumberColumnIndex = getQueryNumberColumnIndex(resultSet);

                iterateLeftRows(leftRowsList, resultRows, resultSet, values, queryNumberColumnIndex);
            }
        } catch (SQLException e) {
            rethrow(e);
        }
        return resultRows;
    }

    private void setObjectsToPreparedStatement(PreparedStatement preparedStatement,
                                               List<JetSqlRow> leftRowsList)
            throws SQLException {
        int[] leftEquiJoinIndices = joinInfo.leftEquiJoinIndices();

        // PreparedStatement parameter index starts from 1
        int parameterIndex = 1;

        // leftRow contains all left table columns used in the select statement
        // leftEquiJoinIndices contains index of columns used in the JOIN clause
        for (JetSqlRow leftRow : leftRowsList) {
            for (int leftEquiJoinIndexValue : leftEquiJoinIndices) {
                Object value = leftRow.get(leftEquiJoinIndexValue);
                preparedStatement.setObject(parameterIndex++, value);
            }
        }
    }

    private void iterateLeftRows(
            List<JetSqlRow> leftRowsList,
            List<JetSqlRow> resultRows,
            ResultSet resultSet,
            Object[] values,
            int queryNumberColumnIndex) throws SQLException {

        Set<Integer> processedQueryNumbers = new HashSet<>();

        boolean moveResultSetForward = true;
        boolean hasNext = false;
        for (int leftRowIndex = 0; leftRowIndex < leftRowsList.size(); leftRowIndex++) {
            JetSqlRow leftRow = leftRowsList.get(leftRowIndex);
            if (moveResultSetForward) {
                hasNext = resultSet.next();
            }
            if (!hasNext) {
                createExtendedRowIfNecessary(leftRow, resultRows);
                moveResultSetForward = false;
                continue;
            }
            do {
                fillValueArray(resultSet, values);
                int queryNumberFromResultSet = resultSet.getInt(queryNumberColumnIndex);
                // We have arrived to new query result
                if (leftRowIndex != queryNumberFromResultSet) {
                    // No need to move the ResultSet forward
                    moveResultSetForward = false;
                    processMismatchingQueryNumber(resultRows, processedQueryNumbers, leftRowIndex, leftRow);
                } else {
                    moveResultSetForward = true;
                    processMatchingQueryNumber(resultRows, values,
                            processedQueryNumbers, leftRowIndex, leftRow);
                }
            } while (moveResultSetForward && (hasNext = resultSet.next()));
        }
    }

    // Called when leftRowIndex is behind the query number or changing to a new query number

    private void processMismatchingQueryNumber(List<JetSqlRow> resultRows,
                                               Set<Integer> processedQueryNumbers,
                                               int leftRowIndex,
                                               JetSqlRow leftRow) {
        // Check if we have processed this leftRow before
        if (!processedQueryNumbers.contains(leftRowIndex)) {
            createExtendedRowIfNecessary(leftRow, resultRows);
            processedQueryNumbers.add(leftRowIndex);
        }
    }

    private void processMatchingQueryNumber(List<JetSqlRow> resultRows,
                                            Object[] values,
                                            Set<Integer> processedQueryNumbers,
                                            int leftRowIndex,
                                            JetSqlRow leftRow) {
        // Join the leftRow with the row from DB
        JetSqlRow jetSqlRowFromDB = new JetSqlRow(expressionEvalContext.getSerializationService(), values);
        JetSqlRow joinedRow = ExpressionUtil.join(leftRow, jetSqlRowFromDB, joinInfo.nonEquiCondition(),
                expressionEvalContext);
        if (joinedRow != null) {
            // The DB row evaluated as true
            resultRows.add(joinedRow);
        } else {
            // TODO no check for if (!joinInfo.isInner())?
            // The DB row evaluated as false
            createExtendedRowIfNecessary(leftRow, resultRows);
        }
        processedQueryNumbers.add(leftRowIndex);
    }

}
