/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.ToResultSetFunction;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * Use {@link SourceProcessors#readJdbcP}.
 */
public final class ReadJdbcP<T> extends AbstractProcessor {

    private final DistributedSupplier<? extends Connection> connectionSupplier;
    private final ToResultSetFunction resultSetFn;
    private final DistributedFunction<? super ResultSet, ? extends T> mapOutputFn;

    private Connection connection;
    private ResultSet resultSet;
    private Traverser traverser;
    private int parallelism;
    private int index;

    private ReadJdbcP(
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull DistributedFunction<? super ResultSet, ? extends T> mapOutputFn
    ) {
        this.connectionSupplier = connectionSupplier;
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
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull DistributedFunction<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(() ->
                new ReadJdbcP<>(connectionSupplier, resultSetFn, mapOutputFn));
    }

    public static <T> ProcessorMetaSupplier supplier(
            @Nonnull String connectionURL,
            @Nonnull String query,
            @Nonnull DistributedFunction<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(() ->
                new ReadJdbcP<>(
                        () -> DriverManager.getConnection(connectionURL),
                        (connection, parallelism, index) -> {
                            PreparedStatement statement = connection.prepareStatement(query);
                            try {
                                return statement.executeQuery();
                            } catch (SQLException e) {
                                statement.close();
                                throw e;
                            }
                        },
                        mapOutputFn)
        ));
    }

    @Override
    protected void init(@Nonnull Context context) {
        this.connection = connectionSupplier.get();
        this.parallelism = context.totalParallelism();
        this.index = context.globalProcessorIndex();
    }

    @Override
    public boolean complete() {
        if (traverser == null) {
            resultSet = uncheckCall(() -> resultSetFn.createResultSet(connection, parallelism, index));
            traverser = ((Traverser<ResultSet>) () -> uncheckCall(() -> resultSet.next() ? resultSet : null))
                    .map(mapOutputFn);
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
}
