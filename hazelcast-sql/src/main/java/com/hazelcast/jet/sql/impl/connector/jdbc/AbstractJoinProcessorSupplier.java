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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.TransformBatchedP;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

abstract class AbstractJoinProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable {

    protected String query;
    protected JetJoinInfo joinInfo;
    protected List<Expression<?>> projections;

    // Transient members are received when ProcessorSupplier is initialized.
    // No need to serialize them
    protected transient ExpressionEvalContext expressionEvalContext;

    AbstractJoinProcessorSupplier() {
    }

    AbstractJoinProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String query,
            @Nonnull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName);
        this.query = query;
        this.joinInfo = joinInfo;
        this.projections = projections;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.expressionEvalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {

        // Return count number of Processors
        return IntStream.range(0, count)
                        .mapToObj(i -> new TransformBatchedP<>(this::joinRows)).
                        collect(toList());
    }

    protected abstract Traverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows);

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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        joinInfo = in.readObject();
        projections = in.readObject();
    }

    protected void createExtendedRowIfNecessary(JetSqlRow leftRow, List<JetSqlRow> jetSqlRows) {
        if (!joinInfo.isInner()) {
            // This is not an inner join, so return a null padded JetSqlRow
            JetSqlRow extendedRow = leftRow.extendedRow(projections.size());
            jetSqlRows.add(extendedRow);
        }
    }

    protected static Object[] createValueArray(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        return new Object[columnCount];
    }

    protected static Object[] createValueArrayExcludingQueryNumber(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        return new Object[columnCount - 1];
    }

    protected static int getQueryNumberColumnIndex(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        return metaData.getColumnCount();
    }

    protected static void fillValueArray(ResultSet resultSet, Object[] values) throws SQLException {
        for (int index = 0; index < values.length; index++) {
            // TODO we need to use mechanism similar (or maybe the same) to valueGetters in SelectProcessorSupplier
            values[index] = resultSet.getObject(index + 1);
        }
    }

}
