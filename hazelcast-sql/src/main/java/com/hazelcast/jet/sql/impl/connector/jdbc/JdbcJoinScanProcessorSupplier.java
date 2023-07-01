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

public class JdbcJoinScanProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements DataSerializable, SecuredFunction {

    transient Context context;

    private String query;

    private JetJoinInfo joinInfo;

    private List<Expression<?>> projections;

    private int batchLimit;

    private transient ExpressionEvalContext evalContext;


    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    public JdbcJoinScanProcessorSupplier() {
    }

    public JdbcJoinScanProcessorSupplier(NestedLoopReaderParams nestedLoopReaderParams, String query) {
        super(nestedLoopReaderParams.getJdbcTable().getDataConnectionName());
        this.query = query;
        this.joinInfo = nestedLoopReaderParams.getJoinInfo();
        this.projections = nestedLoopReaderParams.getProjections();
        this.batchLimit = nestedLoopReaderParams.getJdbcTable().getBatchLimit();
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.context = context;
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        FunctionEx<JetSqlRow, Traverser<JetSqlRow>> functionEx = joinFn(
                dataConnection,
                query,
                joinInfo, evalContext, projections);
        return IntStream.range(0, count)
                .mapToObj(i ->
                        new TransformP<>(functionEx)
                ).
                collect(toList());
        /*
        return IntStream.range(0, count)

                .mapToObj(i ->
                        new ReadJdbcP<>(
                                () -> dataConnection.getConnection(),
                                (con, parallelism, index) -> {
                                    PreparedStatement statement = con.prepareStatement(query);
                                    return statement.executeQuery();
                                },
                                resultSet -> {
                                    Object[] values = new Object[resultSet.getMetaData().getColumnCount()];
                                    for (int index = 0; index < values.length; index++) {
                                        values[index] = resultSet.getObject(index + 1);
                                    }
                                    return new JetSqlRow(evalContext.getSerializationService(), values);
                                }
                        )
                ).
                collect(toList());

         */



        /*for (int i = 0; i < count; i++) {

            FunctionEx<JetSqlRow, Traverser<JetSqlRow>> functionEx = joinFn(joinInfo, evalContext, projections);
            Processor processor = new TransformP<>(functionEx) {
                @Override
                public boolean isCooperative() {
                    return false;
                }
            };
            processors.add(processor);
        }
        return processors;*/

        /*
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            JoinJdbcP processor = new JoinJdbcP<>(
                    query,
                    dataSource,
                    (PreparedStatement ps, JetSqlRow row) -> {
                        for (int j = 0; j < row.getFieldCount(); j++) {
                            // JDBC parameterIndex is 1-based, so j + 1
                            ps.setObject(j + 1, row.get(j));
                        }
                    },
                    SQLExceptionUtils::isNonTransientException,
                    false,
                    batchLimit
            );
            processors.add(processor);
        }
        return processors;*/
    }

    private static FunctionEx<JetSqlRow, Traverser<JetSqlRow>> joinFn(JdbcDataConnection dataConnection,
                                                                      String query,
                                                                      JetJoinInfo joinInfo,
                                                                      ExpressionEvalContext evalContext,
                                                                      List<Expression<?>> projections) {
        return leftRow -> innerJoinFullScan(leftRow, dataConnection, query, projections, joinInfo, evalContext);
    }

    // Left and Right indices are not given and join type is INNER
    private static Traverser<JetSqlRow> innerJoinFullScan(JetSqlRow leftRow,
                                                          JdbcDataConnection dataConnection,
                                                          String query,
                                                          List<Expression<?>> projections,
                                                          JetJoinInfo joinInfo,
                                                          ExpressionEvalContext evalContext) {

        List<JetSqlRow> rows = new ArrayList<>();

        try (Connection connection = dataConnection.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            Object[] values = new Object[resultSet.getMetaData().getColumnCount()];


            while (resultSet.next()) {
                for (int index = 0; index < values.length; index++) {
                    values[index] = resultSet.getObject(index + 1);
                }
                JetSqlRow jetSqlRow = new JetSqlRow(evalContext.getSerializationService(), values);
                JetSqlRow joined = ExpressionUtil.join(leftRow, jetSqlRow, joinInfo.nonEquiCondition(), evalContext);
                if (joined != null) {
                    rows.add(joined);
                } else {
                    if (!joinInfo.isInner()) {
                        JetSqlRow extendedRow = leftRow.extendedRow(projections.size());
                        rows.add(extendedRow);
                    }
                }
            }
        } catch (SQLException sqlException) {
        }
        return traverseIterable(rows);
        /*
        Object[] values = new Object[]{1, "orcun", 200};

        JetSqlRow jetSqlRow = new JetSqlRow(evalContext.getSerializationService(), values);
        JetSqlRow joined = ExpressionUtil.join(leftRow, jetSqlRow, joinInfo.nonEquiCondition(), evalContext);


        if (joined != null) {
            rows.add(joined);
        } else {
            if (!joinInfo.isInner()) {
                leftRow.extendedRow(projections.size());
            }
        }
        return traverseIterable(rows);*/
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        return singletonList(ConnectorPermission.jdbc(dataConnectionName, ACTION_READ));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeString(query);
        out.writeObject(joinInfo);
        out.writeObject(projections);
        out.writeInt(batchLimit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        joinInfo = in.readObject();
        projections = in.readObject();
        batchLimit = in.readInt();
    }
}
