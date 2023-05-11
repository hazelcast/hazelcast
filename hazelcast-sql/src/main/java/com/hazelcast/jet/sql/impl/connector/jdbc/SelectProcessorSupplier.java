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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.ReadJdbcP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class SelectProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private static final Map<Integer, BiFunctionEx<ResultSet, Integer, Object>> GETTERS = new HashMap<>();

    static {
        GETTERS.put(Types.BOOLEAN, ResultSet::getBoolean);
        GETTERS.put(Types.BIT, ResultSet::getBoolean);
        GETTERS.put(Types.TINYINT, ResultSet::getByte);
        GETTERS.put(Types.SMALLINT, ResultSet::getShort);
        GETTERS.put(Types.INTEGER, ResultSet::getInt);
        GETTERS.put(Types.BIGINT, ResultSet::getLong);
        GETTERS.put(Types.VARCHAR, ResultSet::getString);
        GETTERS.put(Types.FLOAT, ResultSet::getFloat);
        GETTERS.put(Types.REAL, ResultSet::getFloat);
        GETTERS.put(Types.DOUBLE, ResultSet::getDouble);
        GETTERS.put(Types.DECIMAL, ResultSet::getBigDecimal);
        GETTERS.put(Types.NUMERIC, ResultSet::getBigDecimal);
        GETTERS.put(Types.DATE, (resultSet, columnIndex) -> resultSet.getDate(columnIndex).toLocalDate());
        GETTERS.put(Types.TIME, (resultSet, columnIndex) -> resultSet.getTime(columnIndex).toLocalTime());
        GETTERS.put(Types.TIMESTAMP, (resultSet, columnIndex) -> resultSet.getTimestamp(columnIndex).toLocalDateTime());
        GETTERS.put(Types.TIMESTAMP_WITH_TIMEZONE, ResultSet::getObject);
    }

    private String query;
    private int[] parameterPositions;

    private transient ExpressionEvalContext evalContext;
    private transient volatile BiFunctionEx<ResultSet, Integer, Object>[] valueGetters;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    public SelectProcessorSupplier(@Nonnull String dataConnectionName,
                                   @Nonnull String query,
                                   @Nonnull int[] parameterPositions) {
        super(dataConnectionName);
        this.query = requireNonNull(query, "query must not be null");
        this.parameterPositions = requireNonNull(parameterPositions, "parameterPositions must not be null");
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        assert count == 1;

        Processor processor = new ReadJdbcP<>(
                () -> dataConnection.getConnection(),
                (connection, parallelism, index) -> {
                    PreparedStatement statement = connection.prepareStatement(query);
                    List<Object> arguments = evalContext.getArguments();
                    for (int j = 0; j < parameterPositions.length; j++) {
                        statement.setObject(j + 1, arguments.get(parameterPositions[j]));
                    }
                    try {
                        ResultSet rs = statement.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();

                        valueGetters = new BiFunctionEx[metaData.getColumnCount()];
                        for (int j = 0; j < metaData.getColumnCount(); j++) {
                            valueGetters[j] = GETTERS.getOrDefault(
                                    metaData.getColumnType(j + 1),
                                    (resultSet, n) -> rs.getObject(n)
                            );
                        }
                        return rs;
                    } catch (SQLException e) {
                        statement.close();
                        throw e;
                    }
                },
                (rs) -> {
                    int columnCount = rs.getMetaData().getColumnCount();
                    Object[] row = new Object[columnCount];
                    for (int j = 0; j < columnCount; j++) {
                        Object value = valueGetters[j].apply(rs, j + 1);
                        row[j] = value;
                    }

                    return new JetSqlRow(evalContext.getSerializationService(), row);
                }
        );

        return singleton(processor);

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
        out.writeIntArray(parameterPositions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        parameterPositions = in.readIntArray();
    }
}
