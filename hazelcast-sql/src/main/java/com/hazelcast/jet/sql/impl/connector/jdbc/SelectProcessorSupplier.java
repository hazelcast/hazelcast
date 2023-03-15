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

public class SelectProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private String query;
    private int[] parameterPositions;

    private transient ExpressionEvalContext evalContext;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    public SelectProcessorSupplier(@Nonnull String dataLinkName,
                                   @Nonnull String query,
                                   @Nonnull int[] parameterPositions) {
        super(dataLinkName);
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
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Processor processor = new ReadJdbcP<>(
                    () -> dataLink.getConnection(),
                    (connection, parallelism, index) -> {
                        PreparedStatement statement = connection.prepareStatement(query);
                        List<Object> arguments = evalContext.getArguments();
                        for (int j = 0; j < parameterPositions.length; j++) {
                            statement.setObject(j + 1, arguments.get(parameterPositions[j]));
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
        return singletonList(ConnectorPermission.jdbc(dataLinkName, ACTION_READ));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataLinkName);
        out.writeString(query);
        out.writeIntArray(parameterPositions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataLinkName = in.readString();
        query = in.readString();
        parameterPositions = in.readIntArray();
    }
}
