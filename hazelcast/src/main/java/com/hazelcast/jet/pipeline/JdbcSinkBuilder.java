/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.connector.DataSourceFromConnectionSupplier;

import javax.annotation.Nonnull;
import javax.sql.CommonDataSource;
import java.sql.PreparedStatement;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * See {@link Sinks#jdbcBuilder()}.
 *
 * @param <T> type of the items the sink accepts
 *
 * @since Jet 4.1
 */
public class JdbcSinkBuilder<T> {
    /**
     * The default setting for whether exactly-once is allowed for the sink.
     */
    public static final boolean DEFAULT_EXACTLY_ONCE = true;

    /**
     * The default batch size limit to use for the sink if batching is supported.
     */
    public static final int DEFAULT_BATCH_LIMIT = 50;

    private String updateQuery;
    private String jdbcUrl;
    private BiConsumerEx<PreparedStatement, T> bindFn;
    private SupplierEx<? extends CommonDataSource> dataSourceSupplier;
    private boolean exactlyOnce = DEFAULT_EXACTLY_ONCE;
    private int batchLimit = DEFAULT_BATCH_LIMIT;

    JdbcSinkBuilder() {
    }

    /**
     * The query to execute for each item. It should contain a parametrized
     * query to which the {@linkplain #bindFn(BiConsumerEx) bind function} will
     * bind values.
     * <p>
     * <b>Which type of statement to use?</b>
     * <p>
     * In exactly-once mode we recommend using an {@code INSERT} statement.
     * Each parallel processor uses a separate transaction: if two processors
     * try to update the same record, the later one will be deadlocked: it will
     * be blocked waiting for a record lock but will never get it because the
     * snapshot will not be able to complete and the lock owner will never
     * commit.
     * <p>
     * On the other hand, in at-least-once mode we recommend using a {@code
     * MERGE} statement. If a unique key is derived from the items, this can
     * give you exactly-once behavior through idempotence without using XA
     * transactions: the MERGE statement will insert a record initially and
     * overwrite the same record, if the job restarted and the same item is
     * written again. If you don't have a unique key in the item, use INSERT
     * with auto-generated key.
     *
     * @param updateQuery the SQL statement to execute for each item
     * @return this instance for fluent API
     */
    @Nonnull
    public JdbcSinkBuilder<T> updateQuery(@Nonnull String updateQuery) {
        this.updateQuery = updateQuery;
        return this;
    }

    /**
     * Set the function to bind values to a {@code PreparedStatement} created
     * with the query set with {@link #updateQuery(String)}. The function
     * should not execute the query, nor call {@code commit()} or any other
     * method.
     *
     * @param bindFn the bind function. The function must be stateless.
     * @return this instance for fluent API
     */
    @Nonnull
    public JdbcSinkBuilder<T> bindFn(@Nonnull BiConsumerEx<PreparedStatement, T> bindFn) {
        this.bindFn = bindFn;
        return this;
    }

    /**
     * Sets the connection URL for the target database.
     * <p>
     * If your job runs in exactly-once mode, don't use this method, but
     * provide an {@link javax.sql.XADataSource} using {@link
     * #dataSourceSupplier(SupplierEx)} method, otherwise the job will fail.
     * If your driver doesn't have an XADataSource implementation, also call
     * {@link #exactlyOnce(boolean) exactlyOnce(false)}.
     * <p>
     * See also {@link #dataSourceSupplier(SupplierEx)}.
     *
     * @param connectionUrl the connection URL
     * @return this instance for fluent API
     */
    @Nonnull
    public JdbcSinkBuilder<T> jdbcUrl(String connectionUrl) {
        this.jdbcUrl = connectionUrl;
        return this;
    }

    /**
     * Sets the supplier of {@link javax.sql.DataSource} or {@link
     * javax.sql.XADataSource}. One dataSource instance will be created on each
     * member. For exactly-once guarantee an XADataSource must be used or the
     * job will fail. If your driver doesn't have an XADataSource
     * implementation, also call {@link #exactlyOnce(boolean) exactlyOnce(false)}.
     * <p>
     * There's no need to use {@link javax.sql.ConnectionPoolDataSource}. One
     * connection is given to each processor and that connection is held during
     * the entire job execution.
     *
     * @param dataSourceSupplier the supplier of data source. The function must
     *     be stateless.
     * @return this instance for fluent API
     */
    @Nonnull
    public JdbcSinkBuilder<T> dataSourceSupplier(SupplierEx<? extends CommonDataSource> dataSourceSupplier) {
        this.dataSourceSupplier = dataSourceSupplier;
        return this;
    }

    /**
     * Sets whether the exactly-once mode is enabled for the sink. If
     * exactly-once is enabled, the job must also be in exactly-once mode for
     * that mode to be used, otherwise the sink will use the job's guarantee.
     * <p>
     * Set exactly-once to false if you want your job to run in exactly-once,
     * but want to reduce the guarantee just for the sink.
     * <p>
     * The default is exactly-once set to {@value DEFAULT_EXACTLY_ONCE}.
     *
     * @param enabled whether exactly-once is allowed for the sink
     * @return this instance for fluent API
     */
    @Nonnull
    public JdbcSinkBuilder<T> exactlyOnce(boolean enabled) {
        this.exactlyOnce = enabled;
        return this;
    }

    /**
     * Sets the batch size limit for the sink. If the JDBC driver supports
     * batched updates, this defines the upper bound to the batch size.
     * <p>
     * The default batch size limit is {@value DEFAULT_BATCH_LIMIT}.
     *
     * @param batchLimit the batch size limit for the sink
     * @return this instance for fluent API
     * @since Jet 4.5
     */
    @Nonnull
    public JdbcSinkBuilder<T> batchLimit(int batchLimit) {
        checkPositive(batchLimit, "batch size limit must be positive");
        this.batchLimit = batchLimit;
        return this;
    }

    /**
     * Creates and returns the JDBC {@link Sink} with the supplied components.
     */
    @Nonnull
    public Sink<T> build() {
        if (dataSourceSupplier == null && jdbcUrl == null) {
            throw new IllegalStateException("Neither jdbcUrl() nor dataSourceSupplier() set");
        }
        if (dataSourceSupplier == null) {
            String connectionUrl = jdbcUrl;
            dataSourceSupplier = () -> new DataSourceFromConnectionSupplier(connectionUrl);
        }
        return Sinks.fromProcessor("jdbcSink",
                SinkProcessors.writeJdbcP(jdbcUrl, updateQuery, dataSourceSupplier, bindFn,
                        exactlyOnce, batchLimit));
    }
}
