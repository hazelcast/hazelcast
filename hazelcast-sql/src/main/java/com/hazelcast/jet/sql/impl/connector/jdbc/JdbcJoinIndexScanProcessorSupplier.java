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

import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.processor.TransformBatchedP;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class JdbcJoinIndexScanProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements DataSerializable, SecuredFunction {

    private JdbcJoinParameters jdbcJoinParameters;

    // Transient members are received when ProcessorSupplier is initialized.
    // No need to serialize them
    private transient ExpressionEvalContext expressionEvalContext;


    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    public JdbcJoinIndexScanProcessorSupplier() {
    }

    public JdbcJoinIndexScanProcessorSupplier(@Nonnull NestedLoopReaderParams nestedLoopReaderParams,
                                              @Nonnull String selectQuery) {
        super(nestedLoopReaderParams.getJdbcTable().getDataConnectionName());
        this.jdbcJoinParameters = new JdbcJoinParameters(selectQuery, nestedLoopReaderParams);
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.expressionEvalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        FunctionEx<Iterable<JetSqlRow>, Traverser<JetSqlRow>> joinFunction = createJoinFunction(
                dataConnection,
                jdbcJoinParameters,
                expressionEvalContext
        );

        // Return count number of Processors
        return IntStream.range(0, count)
                .mapToObj(i -> new TransformBatchedP<>(joinFunction)).
                collect(toList());
    }

    private static FunctionEx<Iterable<JetSqlRow>, Traverser<JetSqlRow>> createJoinFunction(
            JdbcDataConnection jdbcDataConnection,
            JdbcJoinParameters jdbcJoinParameters,
            ExpressionEvalContext expressionEvalContext) {
        return leftRows -> joinRows(leftRows, jdbcDataConnection, jdbcJoinParameters, expressionEvalContext);
    }

    private static Traverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows,
                                                 JdbcDataConnection jdbcDataConnection,
                                                 JdbcJoinParameters jdbcJoinParameters,
                                                 ExpressionEvalContext expressionEvalContext) throws SQLException {

        ArrayList<JetSqlRow> leftRowsList = convertIterableToArrayList(leftRows);
        String unionAllSql = generateSql(jdbcJoinParameters, leftRowsList);

        List<JetSqlRow> resultRows = joinUnionAll(leftRowsList, unionAllSql, jdbcDataConnection, jdbcJoinParameters,
                expressionEvalContext);

        return traverseIterable(resultRows);
    }

    private static String generateSql(JdbcJoinParameters jdbcJoinParameters, ArrayList<JetSqlRow> leftRowsList) {
        String delimiter = "UNION ALL ";
        return IntStream.range(0, leftRowsList.size())
                .mapToObj(i -> {
                    String selectQuery = jdbcJoinParameters.getSelectQuery();
                    return selectQuery.replaceFirst(IndexScanSelectQueryBuilder.ROW_NUMBER_LITERAL, String.valueOf(i));
                })
                .collect(Collectors.joining(delimiter));
    }

    private static <T> ArrayList<T> convertIterableToArrayList(Iterable<T> iterable) {
        Stream<T> stream = StreamSupport.stream(iterable.spliterator(), false);
        return stream.collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<JetSqlRow> joinUnionAll(List<JetSqlRow> leftRowsList,
                                                String unionAllSql,
                                                JdbcDataConnection jdbcDataConnection,
                                                JdbcJoinParameters jdbcJoinParameters,
                                                ExpressionEvalContext expressionEvalContext) throws SQLException {

        List<JetSqlRow> resultRows = new ArrayList<>();

        JetJoinInfo joinInfo = jdbcJoinParameters.getJoinInfo();

        // Index scan : Set the parameters to PreparedStatement and iterate over the ResulSet
        try (Connection connection = jdbcDataConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(unionAllSql)) {

            setObjectsToPreparedStatement(preparedStatement, joinInfo, leftRowsList);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {

                Object[] values = createValueArrayExcludingQueryNumber(resultSet);
                int queryNumberColumnIndex = getQueryNumberColumnIndex(resultSet);

                iterateLeftRows(leftRowsList, expressionEvalContext, resultRows, jdbcJoinParameters,
                        resultSet, values, queryNumberColumnIndex);
            }
        }
        return resultRows;
    }

    private static void iterateLeftRows(List<JetSqlRow> leftRowsList,
                                        ExpressionEvalContext expressionEvalContext,
                                        List<JetSqlRow> resultRows,
                                        JdbcJoinParameters jdbcJoinParameters,
                                        ResultSet resultSet,
                                        Object[] values,
                                        int queryNumberColumnIndex) throws SQLException {

        JetJoinInfo joinInfo = jdbcJoinParameters.getJoinInfo();
        List<Expression<?>> projections = jdbcJoinParameters.getProjections();

        boolean moveResultSetForward = true;
        boolean hasNext = false;
        for (int leftRowIndex = 0; leftRowIndex < leftRowsList.size(); leftRowIndex++) {
            JetSqlRow leftRow = leftRowsList.get(leftRowIndex);
            if (moveResultSetForward) {
                hasNext = resultSet.next();
            }
            if (!hasNext) {
                createExtendedRowIfNecessary(leftRow, projections, joinInfo, resultRows);
                moveResultSetForward = false;
            } else {
                fillValueArray(resultSet, values);
                int queryNumberFromResultSet = resultSet.getInt(queryNumberColumnIndex);
                // We have arrived to new query result
                if (leftRowIndex != queryNumberFromResultSet) {
                    // No need to move the ResultSet forward because we could not process it yet
                    moveResultSetForward = false;
                    createExtendedRowIfNecessary(leftRow, projections, joinInfo, resultRows);
                } else {
                    // Join the leftRow with the row from DB
                    JetSqlRow jetSqlRowFromDB = new JetSqlRow(expressionEvalContext.getSerializationService(), values);
                    JetSqlRow joinedRow = ExpressionUtil.join(leftRow, jetSqlRowFromDB, joinInfo.nonEquiCondition(),
                            expressionEvalContext);
                    if (joinedRow != null) {
                        // The DB row evaluated as true
                        resultRows.add(joinedRow);
                    } else {
                        // The DB row evaluated as false
                        createExtendedRowIfNecessary(leftRow, projections, joinInfo, resultRows);
                    }
                    moveResultSetForward = true;
                }
            }
        }
    }

    private static void setObjectsToPreparedStatement(PreparedStatement preparedStatement,
                                                      JetJoinInfo joinInfo,
                                                      List<JetSqlRow> leftRowsList)
            throws SQLException {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();

        // PreparedStatement parameter index starts from 1
        int objectIndex = 1;

        for (JetSqlRow leftRow : leftRowsList) {
            for (int index = 0; index < rightEquiJoinIndices.length; index++) {
                preparedStatement.setObject(objectIndex++, leftRow.get(index));
            }
        }
    }

    private static void createExtendedRowIfNecessary(JetSqlRow leftRow,
                                                     List<Expression<?>> projections,
                                                     JetJoinInfo joinInfo,
                                                     List<JetSqlRow> jetSqlRows) {
        if (!joinInfo.isInner()) {
            // This is not an inner join, so return a null padded JetSqlRow
            JetSqlRow extendedRow = leftRow.extendedRow(projections.size());
            jetSqlRows.add(extendedRow);
        }
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        return singletonList(ConnectorPermission.jdbc(dataConnectionName, ACTION_READ));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeObject(jdbcJoinParameters);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        jdbcJoinParameters = in.readObject();
    }
}
