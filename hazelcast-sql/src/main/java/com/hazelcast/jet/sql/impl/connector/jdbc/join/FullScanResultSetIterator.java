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

package com.hazelcast.jet.sql.impl.connector.jdbc.join;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.iterator.AutoCloseableIterator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

public class FullScanResultSetIterator<T> implements AutoCloseableIterator<T> {
    private static final ILogger LOGGER = Logger.getLogger(FullScanResultSetIterator.class);
    private final Connection connection;
    private final String sql;
    private final Function<ResultSet, T> rowMapper;
    private final Supplier<T> emptyResultSetMapper;
    private boolean hasNext;
    private boolean hasNextMethodCalled;
    private boolean iteratorClosed;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;
    private boolean callEmptyResultMapper = true;

    private T nextItem;

    public FullScanResultSetIterator(Connection connection,
                                     String sql,
                                     Function<ResultSet, T> rowMapper,
                                     Supplier<T> emptyResultSetMapper
    ) {
        this.connection = connection;
        this.sql = sql;
        this.rowMapper = rowMapper;
        this.emptyResultSetMapper = emptyResultSetMapper;
    }

    @Override
    public boolean hasNext() {
        try {
            if (iteratorClosed) {
                return false;
            }
            getNextItem();
            if (!hasNext) {
                close();
            }
            hasNextMethodCalled = true;
            return hasNext;
        } catch (SQLException sqlException) {
            close();
            throw ExceptionUtil.sneakyThrow(sqlException);
        }
    }

    @Override
    public T next() {
        try {
            if (iteratorClosed) {
                throw new NoSuchElementException();
            }
            // If hasNext() has not been called, get the next item here
            if (!hasNextMethodCalled) {
                getNextItem();
            }
            // If there is no next item, throw NoSuchElementException to comply with the Iterator interface
            if (!hasNext) {
                close();
                throw new NoSuchElementException();
            }
            hasNextMethodCalled = false;
            return nextItem;
        } catch (SQLException sqlException) {
            close();
            throw ExceptionUtil.sneakyThrow(sqlException);
        }
    }

    private void lazyInit() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        }
    }

    @Override
    public void close() {
        if (!iteratorClosed) {
            iteratorClosed = true;
            IOUtil.closeResource(resultSet);
            IOUtil.closeResource(preparedStatement);
            IOUtil.closeResource(connection);
        }
    }

    private void getNextItem() throws SQLException {
        lazyInit();
        hasNext = getNextItemFromEither();
    }

    private boolean getNextItemFromEither() throws SQLException {
        // Classical RowMapper logic.
        // Iterate over the ResultSet and call the RowMapper
        while (resultSet.next()) {
            nextItem = rowMapper.apply(resultSet);
            if (nextItem != null) {
                callEmptyResultMapper = false;
                return true;
            }
        }
        // ResultSet has finished. Checkout if the emptyResultSetMapper should be called
        if (callEmptyResultMapper) {
            callEmptyResultMapper = false;
            nextItem = emptyResultSetMapper.get();
            return nextItem != null;
        }
        return false;
    }
}
