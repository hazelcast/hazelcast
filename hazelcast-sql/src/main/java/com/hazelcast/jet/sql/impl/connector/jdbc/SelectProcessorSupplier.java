/*
 * Copyright 2021 Hazelcast Inc.
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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class SelectProcessorSupplier implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private String jdbcUrl;
    private String tableName;
    private List<String> fields;
    private List<Integer> parameterList;
    private String predicateSql;
    private String projectionSql;

    private transient ExpressionEvalContext evalContext;
    private transient String query;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    public SelectProcessorSupplier(@Nonnull String jdbcUrl,
                                   @Nonnull String tableName,
                                   @Nonnull List<String> fields,
                                   @Nonnull List<Integer> parameterList,
                                   @Nullable String predicateSql,
                                   @Nullable String projectionSql) {
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl must not be null");
        this.tableName = requireNonNull(tableName, "tableName must not be null");
        this.fields = requireNonNull(fields, "fields must not be null");
        this.parameterList = requireNonNull(parameterList, "parameterList must not be null");
        this.predicateSql = predicateSql;
        this.projectionSql = projectionSql;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        evalContext = ExpressionEvalContext.from(context);
        query = buildQuery();
    }

    private String buildQuery() {
        String select;
        if (projectionSql != null) {
            select = projectionSql;
        } else {
            select = "*";
        }
        if (predicateSql != null) {
            return "SELECT " + select + " FROM " + tableName + " WHERE " + predicateSql;
        } else {
            return "SELECT " + select + " FROM " + tableName;
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Processor processor = new ReadJdbcP<>(
                    () -> DriverManager.getConnection(jdbcUrl),
                    (connection, parallelism, index) -> {
                        PreparedStatement statement = connection.prepareStatement(query);
                        List<Object> arguments = evalContext.getArguments();
                        for (int j = 0; j < parameterList.size(); j++) {
                            // TODO is some conversion needed here? maybe for dates (the opposite of convertValue)
                            statement.setObject(j + 1, arguments.get(parameterList.get(j)));
                        }
                        try {
                            return statement.executeQuery();
                        } catch (SQLException e) {
                            statement.close();
                            throw e;
                        }
                    },
                    rs -> {
                        int columnCount = rs.getMetaData().getColumnCount();
                        Object[] row = new Object[columnCount];
                        for (int j = 0; j < columnCount; j++) {
                            Object value = rs.getObject(j + 1);
                            row[j] = convertValue(value);
                        }

                        return new JetSqlRow(evalContext.getSerializationService(), row);
                    }
            );
            processors.add(processor);
        }
        return processors;
    }

    private Object convertValue(Object value) {
        if (value instanceof Date) {
            return ((Date) value).toLocalDate();
        } else if (value instanceof Time) {
            return ((Time) value).toLocalTime();
        } else if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime();
        } else {
            return value;
        }
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        // TODO test security
        return singletonList(ConnectorPermission.jdbc(jdbcUrl, ACTION_READ));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(jdbcUrl);
        out.writeString(tableName);
        out.writeInt(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            out.writeString(fields.get(i));
        }
        out.writeInt(parameterList.size());
        for (int i = 0; i < parameterList.size(); i++) {
            out.writeInt(parameterList.get(i));
        }
        out.writeString(predicateSql);
        out.writeString(projectionSql);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jdbcUrl = in.readString();
        tableName = in.readString();
        int numFields = in.readInt();
        fields = new ArrayList<>(numFields);
        for (int i = 0; i < numFields; i++) {
            fields.add(in.readString());
        }
        int numParameters = in.readInt();
        parameterList = new ArrayList<>(numParameters);
        for (int i = 0; i < numParameters; i++) {
            parameterList.add(in.readInt());
        }
        predicateSql = in.readString();
        projectionSql = in.readString();
    }
}
