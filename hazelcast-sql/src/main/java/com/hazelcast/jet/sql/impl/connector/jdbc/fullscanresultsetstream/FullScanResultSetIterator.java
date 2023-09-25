package com.hazelcast.jet.sql.impl.connector.jdbc.fullscanresultsetstream;

import com.hazelcast.function.SupplierEx;
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
import java.util.function.Function;

public class FullScanResultSetIterator<T> implements Iterator<T> {

    private static final ILogger LOGGER = Logger.getLogger(FullScanResultSetIterator.class);
    private Connection connection;
    private final String sql;
    private final Function<ResultSet, T> rowMapper;
    private final SupplierEx<T> emptyResultSetMapper;
    boolean hasNext;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;
    private boolean callEmptyResultMapper = true;

    T nextItem;

    public FullScanResultSetIterator(Connection connection,
                                     String sql,
                                     Function<ResultSet, T> rowMapper,
                                     SupplierEx<T> emptyResultSetMapper
    ) {
        this.connection = connection;
        this.sql = sql;
        this.rowMapper = rowMapper;
        this.emptyResultSetMapper = emptyResultSetMapper;
    }
    @Override
    public boolean hasNext() {
        try {
            lazyInit();
            hasNext = false;
            if (getNextItemFromRowMapper()) {
                callEmptyResultMapper = false;
                hasNext = true;
            } else {
                if (getNextItemFromEmptyResultSetMapper()) {
                    hasNext = true;
                }
            }
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
        while (resultSet.next()) {
            nextItem = rowMapper.apply(resultSet);
            if (nextItem != null) {
                result = true;
                break;
            }
        }
        return result;
    }
    private boolean getNextItemFromEmptyResultSetMapper() throws SQLException {
        boolean result = false;
        if (callEmptyResultMapper) {
            callEmptyResultMapper = false;
            nextItem = emptyResultSetMapper.get();
            if (nextItem != null) {
                result = true;
            }
        }
        return result;
    }
}
