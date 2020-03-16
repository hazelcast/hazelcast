/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.jdbc;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.jdbc.impl.JdbcCursor;
import com.hazelcast.sql.jdbc.impl.JdbcResultSetAdapter;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Hazelcast JDBC result
 */
@SuppressWarnings({"RedundantThrows", "checkstyle:MethodCount"})
public class HazelcastJdbcResultSet extends JdbcResultSetAdapter {
    /** Parent sstatement. */
    private final HazelcastJdbcStatement statement;

    /** Fetch direction. */
    private int fetchDirection;

    /** Cursor. */
    private JdbcCursor cursor;

    /** Current row. */
    private SqlRow currentRow;

    /** Position of the current row. */
    private int currentRowPosition;

    /** Whether the last read column was null. */
    private boolean wasNull;

    /** Whether the result set is closed. */
    private boolean closed;

    /** Whether the result set is being closed at the moment. Needed to break the loop (rs -> stmt -> rs) */
    private boolean closing;

    public HazelcastJdbcResultSet(
        HazelcastJdbcStatement statement,
        JdbcCursor cursor,
        int fetchDirection
    ) {
        this.statement = statement;
        this.cursor = cursor;
        this.fetchDirection = fetchDirection;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();

        SqlRow row = cursor.getNextRow();

        if (row == null) {
            currentRow = null;
            currentRowPosition = 0;

            return false;
        } else {
            currentRow = row;
            currentRowPosition++;

            return true;
        }
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();

        return currentRowPosition;
    }

    @Override
    public void setFetchSize(int pageSize) throws SQLException {
        checkClosed();

        if (pageSize < 0) {
            throw new SQLException("pageSize cannot be negative: " + pageSize);
        }

        cursor.setPageSize(pageSize);
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();

        return cursor.getPageSize();
    }

    @Override
    public void setFetchDirection(int fetchDirection) throws SQLException {
        checkClosed();

        switch (fetchDirection) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                this.fetchDirection = fetchDirection;

                break;

            default:
                throw new SQLException("Invalid value: " + fetchDirection);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();

        return fetchDirection;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();

        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();

        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (boolean) get(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (byte) get(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (short) get(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (int) get(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (long) get(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (float) get(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions and null-handling
        return (double) get(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions.
        return (BigDecimal) get(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        // TODO: Implement with proper conversions and scale adjustment.
        return (BigDecimal) get(columnIndex);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions.
        return (String) get(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO: Implement with proper conversions
        // TODO: How to use the Calendar properly?
        java.util.Date date = (java.util.Date) get(columnIndex);

        if (date == null) {
            return null;
        }

        return new Date(date.getTime());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO: Implement with proper conversions.
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO: Implement with proper conversions.
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions.
        return get(columnIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        // TODO: Implement with proper conversions.
        return (T) get(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        // TODO: Implement with proper conversions.
        return get(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions.
        return (byte[]) get(columnIndex);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        // TODO: Implement with proper conversions.
        return (URL) get(columnIndex);
    }

    private Object get(int columnIndex) throws SQLException {
        checkClosed();

        if (currentRow == null) {
            throw new SQLException("Result set is not positioned on a row (did you call next()?)");
        }

        int index = columnIndex - 1;

        if (index < 0) {
            throw new SQLException("Column index must be positive: " + columnIndex);
        }

        if (cursor.getColumnCount() <= index) {
            throw new SQLException("Column index is greater than the number of columns in the result set: " + index);
        }

        Object val = currentRow.getObject(index);

        wasNull = val == null;

        return val;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();

        return wasNull;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        // TODO: Implement
        throw new UnsupportedOperationException("Implement me");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        close(true);
    }

    public void close(boolean propagateToParent) throws SQLException {
        if (!closed) {
            if (closing) {
                return;
            }

            closing = true;

            cursor.close();
            cursor = null;

            if (propagateToParent) {
                statement.close();
            }

            closed = true;
        }
    }

    @Override
    public String getCursorName() throws SQLException {
        checkClosed();

        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO: Implement. High priority.
        throw new UnsupportedOperationException("Implement me");
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();

        return statement;
    }
}
