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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class JdbcJoinFullScanProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements DataSerializable, SecuredFunction {

    private JdbcJoinParameters jdbcJoinParameters;

    // Transient members are received when ProcessorSupplier is initialized.
    // No need to serialize them
    private transient ExpressionEvalContext expressionEvalContext;


    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    public JdbcJoinFullScanProcessorSupplier() {
    }

    public JdbcJoinFullScanProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String selectQuery,
            @Nonnull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName);
        this.jdbcJoinParameters = new JdbcJoinParameters(selectQuery, joinInfo, projections);
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
        return IntStream.range(0, count)
                .mapToObj(i -> new TransformBatchedP<>(joinFunction)).
                collect(toList());
    }

    private static FunctionEx<Iterable<JetSqlRow>, Traverser<JetSqlRow>> createJoinFunction(JdbcDataConnection jdbcDataConnection,
                                                                                            JdbcJoinParameters jdbcJoinParameters,
                                                                                            ExpressionEvalContext evalContext) {
        return leftRow -> joinRows(leftRow, jdbcDataConnection, jdbcJoinParameters, evalContext);
    }

    private static Traverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows,
                                                 JdbcDataConnection jdbcDataConnection,
                                                 JdbcJoinParameters jdbcJoinParameters,
                                                 ExpressionEvalContext evalContext) throws SQLException {

        List<JetSqlRow> resultRows = new ArrayList<>();

        for (JetSqlRow leftRow : leftRows) {
            joinRow(leftRow, jdbcDataConnection, jdbcJoinParameters, evalContext, resultRows);
        }
        return traverseIterable(resultRows);
    }

    private static void joinRow(JetSqlRow leftRow,
                                JdbcDataConnection jdbcDataConnection,
                                JdbcJoinParameters jdbcJoinParameters,
                                ExpressionEvalContext evalContext,
                                List<JetSqlRow> resultRows) throws SQLException {
        String query = jdbcJoinParameters.getSelectQuery();
        JetJoinInfo joinInfo = jdbcJoinParameters.getJoinInfo();
        List<Expression<?>> projections = jdbcJoinParameters.getProjections();

        // Full scan : Select * from the table and iterate over the ResulSet
        try (Connection connection = jdbcDataConnection.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            Object[] values = createValueArray(resultSet);

            boolean emptyResultSet = true;

            while (resultSet.next()) {
                emptyResultSet = false;
                fillValueArray(resultSet, values);

                JetSqlRow jetSqlRowFromDB = new JetSqlRow(evalContext.getSerializationService(), values);

                // Join the leftRow with the row from DB
                JetSqlRow joinedRow = ExpressionUtil.join(leftRow, jetSqlRowFromDB, joinInfo.nonEquiCondition(), evalContext);
                if (joinedRow != null) {
                    // The DB row evaluated as true
                    resultRows.add(joinedRow);
                } else {
                    // The DB row evaluated as false
                    if (!joinInfo.isInner()) {
                        // This is not an inner join, so return a null padded JetSqlRow
                        createExtendedRowIfNecessary(leftRow, projections, joinInfo, resultRows);
                    }
                }
            }
            if (emptyResultSet) {
                createExtendedRowIfNecessary(leftRow, projections, joinInfo, resultRows);
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
