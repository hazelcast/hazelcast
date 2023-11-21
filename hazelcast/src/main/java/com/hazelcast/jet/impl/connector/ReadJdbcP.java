/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.connector;

import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.ToResultSetFunction;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.JdbcPropertyKeys;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.StringUtil.isBoolean;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * Use {@link SourceProcessors#readJdbcP}.
 */
public final class ReadJdbcP<T> extends AbstractProcessor {

    private static final ILogger LOGGER = Logger.getLogger(ReadJdbcP.class);

    private final SupplierEx<? extends Connection> newConnectionFn;
    private final ToResultSetFunction resultSetFn;
    private final FunctionEx<? super ResultSet, ? extends T> mapOutputFn;

    private Connection connection;
    private ResultSet resultSet;
    private Traverser<? extends T> traverser;
    private int parallelism;
    private int index;

    public ReadJdbcP(
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        this.newConnectionFn = newConnectionFn;
        this.resultSetFn = resultSetFn;
        this.mapOutputFn = mapOutputFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Use {@link SourceProcessors#readJdbcP}.
     */
    public static <T> ProcessorMetaSupplier supplier(
            @Nonnull SupplierEx<? extends DataSource> newDataSourceFn,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return supplier(ctx -> newDataSourceFn.get().getConnection(), resultSetFn, mapOutputFn);
    }

    /**
     * Use {@link SourceProcessors#readJdbcP}.
     */
    public static <T> ProcessorMetaSupplier supplier(
            @Nonnull FunctionEx<ProcessorSupplier.Context, ? extends Connection> newConnectionFn,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        checkSerializable(newConnectionFn, "newConnectionFn");
        checkSerializable(resultSetFn, "resultSetFn");
        checkSerializable(mapOutputFn, "mapOutputFn");

        // We don't know the JDBC URL yet, so only the 'jdbc:' prefix is used as permission name.
        // Additional permission check with URL retrieved from the JDBC connection metadata
        // is performed in #init(Context) method.
        return ProcessorMetaSupplier.preferLocalParallelismOne(
                readJdbcProcessorFn(newConnectionFn, resultSetFn, mapOutputFn));
    }

    public static <T> ProcessorMetaSupplier supplier(
            @Nonnull String connectionURL,
            @Nonnull String query,
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        checkSerializable(mapOutputFn, "mapOutputFn");

        return ProcessorMetaSupplier.forceTotalParallelismOne(
                readJdbcProcessorFn(
                        // Return a new connection. Connection will be closed by ReadJdbcP processor
                        context -> DriverManager.getConnection(connectionURL),
                        // Create a ResultSet. ResultSet will be closed by ReadJdbcP processor
                        (connection, parallelism, index) -> {
                            setAutoCommitIfNecessary(connection, properties);
                            PreparedStatement preparedStatement = connection.prepareStatement(query);
                            try {
                                setFetchSizeIfNecessary(preparedStatement, properties);
                                return preparedStatement.executeQuery();
                            } catch (SQLException e) {
                                preparedStatement.close();
                                throw e;
                            }
                        },
                        mapOutputFn),
                newUnsecureUuidString()
        );
    }

    public static <T> ProcessorMetaSupplier supplier(
            DataConnectionRef dataConnectionRef,
            ToResultSetFunction resultSetFn,
            FunctionEx<? super ResultSet, ? extends T> mapOutputFn) {

        return ProcessorMetaSupplier.preferLocalParallelismOne(
                new ProcessorSupplier() {
                    private static final long serialVersionUID = 1L;

                    private transient JdbcDataConnection dataConnection;

                    @Override
                    public void init(@Nonnull Context context) {
                        dataConnection = context.dataConnectionService()
                                .getAndRetainDataConnection(dataConnectionRef.getName(), JdbcDataConnection.class);
                    }

                    @Nonnull
                    @Override
                    public Collection<? extends Processor> get(int count) {
                        return IntStream.range(0, count)
                                .mapToObj(i -> new ReadJdbcP<T>(() -> dataConnection.getConnection(), resultSetFn, mapOutputFn))
                                .collect(Collectors.toList());
                    }

                    @Override
                    public void close(@Nullable Throwable error) {
                        if (dataConnection != null) {
                            dataConnection.release();
                        }
                    }
                });
    }

    private static <T> ProcessorSupplier readJdbcProcessorFn(
            FunctionEx<ProcessorSupplier.Context, ? extends Connection> newConnectionFn,
            ToResultSetFunction resultSetFn,
            FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return new ProcessorSupplier() {
            private static final long serialVersionUID = 1L;

            private transient Context context;

            @Override
            public void init(@Nonnull ProcessorSupplier.Context context) {
                this.context = context;
            }

            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                return IntStream.range(0, count)
                        .mapToObj(i -> new ReadJdbcP<T>(() -> newConnectionFn.apply(context), resultSetFn, mapOutputFn))
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    protected void init(@Nonnull Context context) {
        // workaround for https://github.com/hazelcast/hazelcast-jet/issues/2603
        DriverManager.getDrivers();
        this.connection = newConnectionFn.get();
        this.parallelism = context.totalParallelism();
        this.index = context.globalProcessorIndex();
    }

    @Override
    public boolean complete() {
        if (traverser == null) {
            resultSet = uncheckCall(() -> resultSetFn.createResultSet(connection, parallelism, index));
            Traverser<ResultSet> t = () -> uncheckCall(() -> resultSet.next() ? resultSet : null);
            traverser = t.map(mapOutputFn);
        }
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() throws Exception {
        Exception resultSetException = null;
        Exception statementException = null;
        if (resultSet != null) {
            Statement statement = resultSet.getStatement();
            resultSetException = close(resultSet);
            if (statement != null) {
                statementException = close(statement);
            }
        }
        if (connection != null) {
            connection.close();
        }
        if (resultSetException != null) {
            throw resultSetException;
        }
        if (statementException != null) {
            throw statementException;
        }
    }

    private static Exception close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            return e;
        }
        return null;
    }

    private static void setAutoCommitIfNecessary(Connection connection, Properties properties) throws SQLException {
        String key = JdbcPropertyKeys.AUTO_COMMIT;
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            if (isBoolean(value)) {
                boolean autoCommit = Boolean.parseBoolean(value);
                connection.setAutoCommit(autoCommit);
            } else {
                throw new IllegalArgumentException("Invalid boolean value specified for autoCommit: " + value);
            }
        }
    }

    private static void setFetchSizeIfNecessary(PreparedStatement statement, Properties properties) throws SQLException {
        String key = JdbcPropertyKeys.FETCH_SIZE;
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            try {
                int fetchSize = Integer.parseInt(value);
                statement.setFetchSize(fetchSize);
            } catch (NumberFormatException exception) {
                LOGGER.severe("Invalid integer value specified for fetchSize: " + value, exception);
                throw exception;
            }
        }
    }
}
