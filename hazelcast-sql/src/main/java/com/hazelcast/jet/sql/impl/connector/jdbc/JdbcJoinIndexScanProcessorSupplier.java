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
import com.hazelcast.jet.impl.processor.TransformP;
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
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class JdbcJoinIndexScanProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements DataSerializable, SecuredFunction {

    private String selectQuery;

    private JetJoinInfo joinInfo;

    private List<Expression<?>> projections;

    // Transient members are received when ProcessorSupplier is initialized.
    // No need to serialize them


    private transient ExpressionEvalContext expressionEvalContext;


    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    public JdbcJoinIndexScanProcessorSupplier() {
    }

    public JdbcJoinIndexScanProcessorSupplier(@Nonnull NestedLoopReaderParams nestedLoopReaderParams,
                                              @Nonnull String selectQuery) {
        super(nestedLoopReaderParams.getJdbcTable().getDataConnectionName());
        this.selectQuery = selectQuery;
        this.joinInfo = nestedLoopReaderParams.getJoinInfo();
        this.projections = nestedLoopReaderParams.getProjections();
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.expressionEvalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        FunctionEx<JetSqlRow, Traverser<JetSqlRow>> joinFunction = createJoinFunction(
                dataConnection,
                selectQuery,
                joinInfo,
                expressionEvalContext,
                projections);
        return IntStream.range(0, count)
                .mapToObj(i -> new TransformP<>(joinFunction)).
                collect(toList());
    }

    private static FunctionEx<JetSqlRow, Traverser<JetSqlRow>> createJoinFunction(JdbcDataConnection jdbcDataConnection,
                                                                                  String query,
                                                                                  JetJoinInfo joinInfo,
                                                                                  ExpressionEvalContext evalContext,
                                                                                  List<Expression<?>> projections) {
        return leftRow -> fullScanJoin(leftRow, jdbcDataConnection, query, projections, joinInfo, evalContext);
    }

    private static Traverser<JetSqlRow> fullScanJoin(JetSqlRow leftRow,
                                                     JdbcDataConnection jdbcDataConnection,
                                                     String query,
                                                     List<Expression<?>> projections,
                                                     JetJoinInfo joinInfo,
                                                     ExpressionEvalContext evalContext) throws SQLException {

        List<JetSqlRow> jetSqlRows = new ArrayList<>();

        try (Connection connection = jdbcDataConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            setObjects(preparedStatement, joinInfo, leftRow);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {

                Object[] values = getValueArray(resultSet);

                boolean emptyResultSet = true;

                while (resultSet.next()) {
                    emptyResultSet = false;
                    fillValueArray(resultSet, values);

                    JetSqlRow jetSqlRow = new JetSqlRow(evalContext.getSerializationService(), values);
                    JetSqlRow joinedRow = ExpressionUtil.join(leftRow, jetSqlRow, joinInfo.nonEquiCondition(), evalContext);
                    if (joinedRow != null) {
                        // The DB row evaluated as true
                        jetSqlRows.add(joinedRow);
                    } else {
                        // The DB row evaluated as false
                        createExtendedRowIfNecessary(leftRow, projections, joinInfo, jetSqlRows);
                    }
                }
                if (emptyResultSet) {
                    createExtendedRowIfNecessary(leftRow, projections, joinInfo, jetSqlRows);
                }
            }
        }
        return traverseIterable(jetSqlRows);
    }

    private static void setObjects(PreparedStatement preparedStatement, JetJoinInfo joinInfo, JetSqlRow leftRow)
            throws SQLException {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int index = 0; index < rightEquiJoinIndices.length; index++) {
            preparedStatement.setObject(index + 1, leftRow.get(index));
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
        out.writeString(selectQuery);
        out.writeObject(joinInfo);
        out.writeObject(projections);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        selectQuery = in.readString();
        joinInfo = in.readObject();
        projections = in.readObject();
    }
}
