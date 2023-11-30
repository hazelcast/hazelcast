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

package com.hazelcast.mapstore;

import com.hazelcast.sql.SqlColumnMetadata;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.hazelcast.mapstore.GenericMapLoader.COLUMNS_PROPERTY;

class Queries {

    private static final SqlDialect DIALECT = CalciteSqlDialect.DEFAULT;

    private final String loadQuery;

    private final Function<Integer, String> loadAllFactory;
    private final Map<Integer, String> loadAllQueries = new ConcurrentHashMap<>();

    private final String loadAllKeys;

    private String storeSink;
    private final String storeUpdate;
    private final String delete;

    private final Function<Integer, String> deleteAllFactory;
    private final Map<Integer, String> deleteAllQueries = new ConcurrentHashMap<>();

    private int columnSize = 0;
    private Set<String> allColumns;

    private String idColumn;

    private String mapping;

    private List<SqlColumnMetadata> columnMetadata;

    Queries(String mapping, String idColumn, List<SqlColumnMetadata> columnMetadata) {
        this.columnMetadata = columnMetadata;

        this.mapping = mapping;

        this.idColumn = idColumn;

        loadQuery = buildLoadQuery(mapping, idColumn);

        loadAllFactory = n -> buildLoadAllQuery(mapping, idColumn, n);

        loadAllKeys = buildLoadAllKeysQuery(mapping, idColumn);

        storeSink = buildStoreSinkQuery(mapping, columnMetadata);

        storeUpdate = buildStoreUpdateQuery(mapping, idColumn, columnMetadata);

        delete = buildDeleteQuery(mapping, idColumn);

        deleteAllFactory = n -> buildDeleteAllQuery(mapping, idColumn, n);
    }

    private static String buildLoadQuery(String mapping, String idColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ");
        DIALECT.quoteIdentifier(sb, mapping);
        sb.append(" WHERE ");
        DIALECT.quoteIdentifier(sb, idColumn);
        sb.append(" = ?");
        return sb.toString();
    }

    private String buildLoadAllQuery(String mapping, String idColumn, int n) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ");
        DIALECT.quoteIdentifier(sb, mapping);
        sb.append(" WHERE ");
        DIALECT.quoteIdentifier(sb, idColumn);
        sb.append(" IN (");
        appendQueryParams(sb, n);
        sb.append(')');
        return sb.toString();
    }

    private static String buildLoadAllKeysQuery(String mapping, String idColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        DIALECT.quoteIdentifier(sb, idColumn);
        sb.append(" FROM ");
        DIALECT.quoteIdentifier(sb, mapping);
        return sb.toString();
    }

    private String buildStoreSinkQuery(String mapping, List<SqlColumnMetadata> columnMetadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("SINK INTO ");
        DIALECT.quoteIdentifier(sb, mapping);
        sb.append(" (");
        int defaultCount = 0;
        if (columnSize != 0) {
            defaultCount = columnMetadata.size() - allColumns.size();
            setColumnsWithDefaultValues(sb, columnMetadata);
        } else {
            for (Iterator<SqlColumnMetadata> iterator = columnMetadata.iterator(); iterator.hasNext(); ) {
                SqlColumnMetadata column = iterator.next();
                DIALECT.quoteIdentifier(sb, column.getName());
                if (iterator.hasNext()) {
                    sb.append(", ");
                }
            }
        }

        sb.append(") VALUES (");
        appendQueryParams(sb, columnMetadata.size() - defaultCount);
        sb.append(')');
        return sb.toString();
    }

    private String buildStoreUpdateQuery(String mapping, String idColumn, List<SqlColumnMetadata> columnMetadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        DIALECT.quoteIdentifier(sb, mapping);
        sb.append(" SET ");
        for (Iterator<SqlColumnMetadata> iterator = columnMetadata.iterator(); iterator.hasNext(); ) {
            SqlColumnMetadata column = iterator.next();
            if (idColumn.equals(column.getName())) {
                continue;
            }
            DIALECT.quoteIdentifier(sb, column.getName());
            sb.append(" = ?");
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(" WHERE ");
        DIALECT.quoteIdentifier(sb, idColumn);
        sb.append(" = ?");
        return sb.toString();
    }

    private static String buildDeleteQuery(String mapping, String idColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        DIALECT.quoteIdentifier(sb, mapping);
        sb.append(" WHERE ");
        DIALECT.quoteIdentifier(sb, idColumn);
        sb.append(" = ?");
        return sb.toString();
    }

    private static void appendQueryParams(StringBuilder sb, int n) {
        for (int i = 0; i < n; i++) {
            sb.append('?');
            if (i < (n - 1)) {
                sb.append(", ");
            }
        }
    }

    private String buildDeleteAllQuery(String mapping, String idColumn, int n) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        DIALECT.quoteIdentifier(sb, mapping);
        sb.append(" WHERE ");
        DIALECT.quoteIdentifier(sb, idColumn);
        sb.append(" IN (");
        appendQueryParams(sb, n);
        sb.append(")");
        return sb.toString();
    }

    private void setColumnsWithDefaultValues(StringBuilder sb, List<SqlColumnMetadata> columnMetadataList) {
        boolean firstColumn = true;
        for (Iterator<SqlColumnMetadata> iterator = columnMetadataList.iterator(); iterator.hasNext();) {
            SqlColumnMetadata column = iterator.next();
            if (allColumns.contains(column.getName())) {
                if (iterator.hasNext() && !firstColumn) {
                    sb.append(", ");
                }
                DIALECT.quoteIdentifier(sb, column.getName());
                firstColumn = false;
            }
        }
    }

    String load() {
        return loadQuery;
    }

    String loadAll(int n) {
        return loadAllQueries.computeIfAbsent(n, loadAllFactory);
    }

    String loadAllKeys() {
        return loadAllKeys;
    }

    String storeSink() {
        return storeSink;
    }

    String storeUpdate() {
        return storeUpdate;
    }


    String delete() {
        return delete;
    }

    String deleteAll(int n) {
        return deleteAllQueries.computeIfAbsent(n, deleteAllFactory);
    }

    public void recreateStoreSink(Set<String> allColumns, int size) {
        this.allColumns = allColumns;
        columnSize = size;
        storeSink = buildStoreSinkQuery(mapping, columnMetadata);
    }

}
