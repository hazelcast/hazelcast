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

package com.hazelcast.jet.sql.impl.connector.jdbc.joinindexscanresultsetstream;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.sql.HazelcastSqlException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

public class JoinIndexScanResultSetIterator<T> implements Iterator<T> {
    private static final ILogger LOGGER = Logger.getLogger(JoinIndexScanResultSetIterator.class);
    private Connection connection;
    private final String sql;
    private final Function<ResultSet, T> rowMapper;
    private final Consumer<PreparedStatement> preparedStatementSetter;
    private boolean hasNext;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;
    private T nextItem;

    public JoinIndexScanResultSetIterator(Connection connection,
                                          String sql,
                                          Function<ResultSet, T> rowMapper,
                                          Consumer<PreparedStatement> preparedStatementSetter) {
        this.connection = connection;
        this.sql = sql;
        this.rowMapper = rowMapper;
        this.preparedStatementSetter = preparedStatementSetter;
    }
    @Override
    public boolean hasNext() {
        try {
            lazyInit();
            hasNext = getNextItemFromRowMapper();
            if (!hasNext) {
                close();
            }
            return hasNext;
        } catch (SQLException sqlException) {
            close();
            throw new HazelcastSqlException("Error occurred while iterating ResultSet", sqlException);
        }
    }
    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        return nextItem;
    }
    private void lazyInit() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatementSetter.accept(preparedStatement);
            resultSet = preparedStatement.executeQuery();
        }
    }
    private void close() {
        LOGGER.info("Close is called");
        IOUtil.closeResource(resultSet);
        resultSet = null;
        IOUtil.closeResource(preparedStatement);
        preparedStatement = null;
        IOUtil.closeResource(connection);
        connection = null;
    }
    private boolean getNextItemFromRowMapper() throws SQLException {
        boolean result = false;
        nextItem = rowMapper.apply(resultSet);
        if (nextItem != null) {
            result = true;
        }
        return result;
    }
}
