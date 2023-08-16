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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
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

    private Map<String, BiFunctionEx<ResultSet, Integer, Object>> getters;

    private String query;
    private int[] parameterPositions;

    private transient ExpressionEvalContext evalContext;
    private transient volatile BiFunctionEx<ResultSet, Integer, Object>[] valueGetters;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    public SelectProcessorSupplier(@Nonnull String dataConnectionName,
                                   @Nonnull String query,
                                   @Nonnull int[] parameterPositions,
                                   SqlDialect dialect) {
        super(dataConnectionName);
        this.query = requireNonNull(query, "query must not be null");
        this.parameterPositions = requireNonNull(parameterPositions, "parameterPositions must not be null");
        this.getters = initializeGetters(dialect);
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
            valueGetters[j] = getters.getOrDefault(
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
        out.writeObject(getters);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        parameterPositions = in.readIntArray();
        getters = in.readObject();
    }

    private static Map<String, BiFunctionEx<ResultSet, Integer, Object>> initializeGetters(SqlDialect sqldialect) {
        Map<String, BiFunctionEx<ResultSet, Integer, Object>> getters = new HashMap<>();
        getters.put("BOOLEAN", ResultSet::getBoolean);
        getters.put("BOOL", ResultSet::getBoolean);
        getters.put("BIT", ResultSet::getBoolean);

        getters.put("TINYINT", ResultSet::getByte);

        getters.put("SMALLINT", ResultSet::getShort);
        getters.put("INT2", ResultSet::getShort);

        getters.put("INT", ResultSet::getInt);
        getters.put("INT4", ResultSet::getInt);
        getters.put("INTEGER", ResultSet::getInt);

        getters.put("INT8", ResultSet::getLong);
        getters.put("BIGINT", ResultSet::getLong);

        getters.put("VARCHAR", ResultSet::getString);
        getters.put("CHARACTER VARYING", ResultSet::getString);
        getters.put("TEXT", ResultSet::getString);

        getters.put("REAL", ResultSet::getFloat);
        getters.put("FLOAT4", ResultSet::getFloat);

        getters.put("DOUBLE", ResultSet::getDouble);
        getters.put("DOUBLE PRECISION", ResultSet::getDouble);
        getters.put("DECIMAL", ResultSet::getBigDecimal);
        getters.put("NUMERIC", ResultSet::getBigDecimal);

        getters.put("DATE", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDate.class));
        getters.put("TIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalTime.class));

        if (sqldialect instanceof MssqlSqlDialect) {
            getters.put("FLOAT", ResultSet::getDouble);
            getters.put("DATETIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
            getters.put("DATETIMEOFFSET", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
        } else {
            getters.put("FLOAT", ResultSet::getFloat);
            getters.put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
            getters.put("TIMESTAMP_WITH_TIMEZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
        }
        return getters;
    }
}
