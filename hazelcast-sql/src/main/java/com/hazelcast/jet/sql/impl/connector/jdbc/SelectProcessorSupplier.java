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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
public class SelectProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private static final Map<String, BiFunctionEx<ResultSet, Integer, Object>> GETTERS = new HashMap<>();

    static {
        GETTERS.put("BOOLEAN", ResultSet::getBoolean);
        GETTERS.put("BOOL", ResultSet::getBoolean);
        GETTERS.put("BIT", ResultSet::getBoolean);

        GETTERS.put("TINYINT", ResultSet::getByte);

        GETTERS.put("SMALLINT", ResultSet::getShort);
        GETTERS.put("INT2", ResultSet::getShort);

        GETTERS.put("INT", ResultSet::getInt);
        GETTERS.put("INT4", ResultSet::getInt);
        GETTERS.put("INTEGER", ResultSet::getInt);

        GETTERS.put("INT8", ResultSet::getLong);
        GETTERS.put("BIGINT", ResultSet::getLong);

        GETTERS.put("VARCHAR", ResultSet::getString);
        GETTERS.put("CHARACTER VARYING", ResultSet::getString);
        GETTERS.put("TEXT", ResultSet::getString);

        GETTERS.put("REAL", ResultSet::getFloat);
        GETTERS.put("FLOAT", ResultSet::getFloat);
        GETTERS.put("FLOAT4", ResultSet::getFloat);

        GETTERS.put("DOUBLE", ResultSet::getDouble);
        GETTERS.put("DOUBLE PRECISION", ResultSet::getDouble);
        GETTERS.put("DECIMAL", ResultSet::getBigDecimal);
        GETTERS.put("NUMERIC", ResultSet::getBigDecimal);

        GETTERS.put("DATE", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDate.class));
        GETTERS.put("TIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalTime.class));
        GETTERS.put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        GETTERS.put("TIMESTAMP_WITH_TIMEZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
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
                        valueGetters = prepareValueGettersFromMetadata(rs);
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

    private BiFunctionEx<ResultSet, Integer, Object>[] prepareValueGettersFromMetadata(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();

        BiFunctionEx<ResultSet, Integer, Object>[] valueGetters = new BiFunctionEx[metaData.getColumnCount()];
        for (int j = 0; j < metaData.getColumnCount(); j++) {
            String type = metaData.getColumnTypeName(j + 1).toUpperCase(Locale.ROOT);
            valueGetters[j] = GETTERS.getOrDefault(
                    type,
                    (resultSet, n) -> rs.getObject(n)
            );
        }
        return valueGetters;
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
