/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Use {@link SinkProcessors#writeJdbcP}.
 */
public final class WriteJdbcP<T> implements Processor {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, SECONDS.toNanos(1), SECONDS.toNanos(10));
    private static final int BATCH_LIMIT = 50;

    private final DistributedSupplier<? extends Connection> connectionSupplier;
    private final DistributedBiConsumer<? super PreparedStatement, ? super T> bindFn;
    private final String updateQuery;

    private ILogger logger;
    private Connection connection;
    private PreparedStatement statement;
    private List<T> itemList = new ArrayList<>();
    private int idleCount;
    private boolean supportsBatch;
    private int batchCount;

    private WriteJdbcP(
            @Nonnull String updateQuery,
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull DistributedBiConsumer<? super PreparedStatement, ? super T> bindFn
    ) {
        this.updateQuery = updateQuery;
        this.connectionSupplier = connectionSupplier;
        this.bindFn = bindFn;
    }

    /**
     * Use {@link SinkProcessors#writeJdbcP}.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String updateQuery,
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull DistributedBiConsumer<? super PreparedStatement, ? super T> bindFn

    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(() ->
                new WriteJdbcP<>(updateQuery, connectionSupplier, bindFn));
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        connectAndPrepareStatement();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drainTo(itemList);
        while (!itemList.isEmpty()) {
            if (!reconnectIfNecessary()) {
                continue;
            }
            try {
                for (T item : itemList) {
                    bindFn.accept(statement, item);
                    addBatchOrExecute();
                }
                executeBatch();
                connection.commit();
                itemList.clear();
                idleCount = 0;
            } catch (Exception e) {
                if (e instanceof SQLNonTransientException ||
                        e.getCause() instanceof SQLNonTransientException) {
                    throw ExceptionUtil.rethrow(e);
                } else {
                    logger.warning("Exception during update", e.getCause());
                    idleCount++;
                }
            }
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void close() {
        closeWithLogging(statement);
        closeWithLogging(connection);
    }

    private boolean connectAndPrepareStatement() {
        try {
            connection = connectionSupplier.get();
            connection.setAutoCommit(false);
            supportsBatch = connection.getMetaData().supportsBatchUpdates();
            statement = connection.prepareStatement(updateQuery);
        } catch (Exception e) {
            logger.warning("Exception during connecting and preparing the statement", e);
            idleCount++;
            return false;
        }
        return true;
    }

    private void addBatchOrExecute() throws SQLException {
        if (!supportsBatch) {
            statement.executeUpdate();
            return;
        }
        statement.addBatch();
        if (++batchCount == BATCH_LIMIT) {
            statement.executeBatch();
            batchCount = 0;
        }

    }

    private void executeBatch() throws SQLException {
        if (supportsBatch) {
            statement.executeBatch();
            batchCount = 0;
        }
    }

    private boolean reconnectIfNecessary() {
        if (idleCount == 0) {
            return true;
        }
        IDLER.idle(idleCount);

        close();

        return connectAndPrepareStatement();
    }

    private void closeWithLogging(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warning("Exception during closing " + closeable, e);
        }
    }
}
