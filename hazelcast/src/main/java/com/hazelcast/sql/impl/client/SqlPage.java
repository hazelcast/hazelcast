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

package com.hazelcast.sql.impl.client;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlRowImpl;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A finite set of rows returned to the client.
 */
public final class SqlPage {

    private final List<SqlColumnType> columnTypes;
    private final DataHolder data;
    private final boolean last;

    private SqlPage(
        List<SqlColumnType> columnTypes,
        DataHolder data,
        boolean last
    ) {
        this.columnTypes = columnTypes;
        this.data = data;
        this.last = last;
    }

    public static SqlPage fromRows(
        List<SqlColumnType> columnTypes,
        List<SqlRow> rows,
        boolean last,
        InternalSerializationService serializationService
    ) {
        return new SqlPage(columnTypes, new RowsetDataHolder(rows, serializationService), last);
    }

    public static SqlPage fromColumns(List<SqlColumnType> columnTypes, List<List<?>> columns, boolean last) {
        return new SqlPage(columnTypes, new ColumnarDataHolder(columns), last);
    }

    public int getRowCount() {
        return data.getRowCount();
    }

    public int getColumnCount() {
        return columnTypes.size();
    }

    public List<SqlColumnType> getColumnTypes() {
        return columnTypes;
    }

    public Object getColumnValueForClient(int columnIndex, int rowIndex) {
        assert columnIndex < getColumnCount();
        assert rowIndex < getRowCount();

        return data.getColumnValueForClient(columnIndex, rowIndex);
    }

    public Iterable<?> getColumnValuesForServer(int columnIndex) {
        assert columnIndex < getColumnCount();

        SqlColumnType columnType = columnTypes.get(columnIndex);

        return data.getColumnValuesForServer(columnIndex, columnType);
    }

    public boolean isLast() {
        return last;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlPage page = (SqlPage) o;

        return last == page.last && columnTypes.equals(page.columnTypes) && data.equals(page.data);
    }

    @Override
    public int hashCode() {
        int result = columnTypes.hashCode();
        result = 31 * result + data.hashCode();
        result = 31 * result + (last ? 1 : 0);
        return result;
    }

    private interface DataHolder {
        int getRowCount();
        Object getColumnValueForClient(int columnIndex, int rowIndex);
        Iterable<?> getColumnValuesForServer(int columnIndex, SqlColumnType columnType);
    }

    private static final class RowsetDataHolder implements DataHolder {

        private final List<SqlRow> rows;
        private final InternalSerializationService serializationService;

        private RowsetDataHolder(List<SqlRow> rows, InternalSerializationService serializationService) {
            this.rows = rows;
            this.serializationService = serializationService;
        }

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public Object getColumnValueForClient(int columnIndex, int rowIndex) {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Iterable<Object> getColumnValuesForServer(int columnIndex, SqlColumnType columnType) {
            if (columnType == SqlColumnType.NULL) {
                return new NullTypeIterable(getRowCount());
            } else {
                boolean convertToData = convertToData(columnType);

                return new RowsetColumnIterable(rows, serializationService, columnIndex, convertToData);
            }
        }
    }

    private static final class ColumnarDataHolder implements DataHolder {

        private final List<List<?>> columns;

        private ColumnarDataHolder(List<List<?>> columns) {
            this.columns = columns;
        }

        @Override
        public int getRowCount() {
            return columns.get(0).size();
        }

        @Override
        public Object getColumnValueForClient(int columnIndex, int rowIndex) {
            return columns.get(columnIndex).get(rowIndex);
        }

        /**
         * Used for testing only.
         */
        @Override
        public Iterable<?> getColumnValuesForServer(int columnIndex, SqlColumnType columnType) {
            assert !convertToData(columnType);

            return columns.get(columnIndex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ColumnarDataHolder that = (ColumnarDataHolder) o;

            return columns.equals(that.columns);
        }

        @Override
        public int hashCode() {
            return columns.hashCode();
        }
    }

    private static final class NullTypeIterable implements Iterable<Object> {

        private final int count;

        private NullTypeIterable(int count) {
            this.count = count;
        }

        @Nonnull
        @Override
        public Iterator<Object> iterator() {
            return new NullTypeIterator(count);
        }
    }

    private static final class NullTypeIterator implements Iterator<Object> {

        private final int count;
        private int position;

        private NullTypeIterator(int count) {
            this.count = count;
        }

        @Override
        public boolean hasNext() {
            return position < count;
        }

        @Override
        public Object next() {
            if (position == count) {
                throw new NoSuchElementException();
            } else {
                position++;

                return null;
            }
        }
    }

    private static final class RowsetColumnIterable implements Iterable<Object> {

        private final List<SqlRow> rows;
        private final InternalSerializationService serializationService;
        private final int columnIndex;
        private final boolean convertToData;

        private RowsetColumnIterable(
            List<SqlRow> rows,
            InternalSerializationService serializationService,
            int columnIndex,
            boolean convertToData
        ) {
            this.rows = rows;
            this.serializationService = serializationService;
            this.columnIndex = columnIndex;
            this.convertToData = convertToData;
        }

        @Nonnull
        @Override
        public Iterator<Object> iterator() {
            return new RowsetColumnIterator(rows, serializationService, columnIndex, convertToData);
        }
    }

    private static final class RowsetColumnIterator implements Iterator<Object> {

        private final List<SqlRow> rows;
        private final InternalSerializationService serializationService;
        private final int columnIndex;
        private final boolean convertToData;
        private final int count;
        private int position;

        private RowsetColumnIterator(
            List<SqlRow> rows,
            InternalSerializationService serializationService,
            int columnIndex,
            boolean convertToData
        ) {
            this.rows = rows;
            this.serializationService = serializationService;
            this.columnIndex = columnIndex;
            this.convertToData = convertToData;

            count = rows.size();
        }

        @Override
        public boolean hasNext() {
            return position < count;
        }

        @Override
        public Object next() {
            if (position == count) {
                throw new NoSuchElementException();
            } else {
                Object res;
                if (convertToData) {
                    res = ((SqlRowImpl) rows.get(position)).getObjectRaw(columnIndex);
                    res = serializationService.toData(res);
                } else {
                    res = rows.get(position).getObject(columnIndex);
                }

                position++;
                return res;
            }
        }
    }

    public static boolean convertToData(SqlColumnType type) {
        return type == SqlColumnType.OBJECT;
    }
}
