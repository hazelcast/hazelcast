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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

class Queries {

    private final String loadQuery;

    private final Function<Integer, String> loadAllFactory;
    private final Map<Integer, String> loadAllQueries = new ConcurrentHashMap<>();

    private final String loadAllKeys;

    private final String storeSink;
    private final String storeUpdate;
    private final String delete;

    private final Function<Integer, String> deleteAllFactory;
    private final Map<Integer, String> deleteAllQueries = new ConcurrentHashMap<>();

    Queries(String mapping, String idColumn, List<SqlColumnMetadata> columnMetadata) {
        loadQuery = String.format("SELECT * FROM \"%s\" WHERE \"%s\" = ?", mapping, idColumn);

        loadAllFactory = n -> String.format("SELECT * FROM \"%s\" WHERE \"%s\" IN (%s)", mapping, idColumn,
                queryParams(n));

        loadAllKeys = "SELECT \"" + idColumn + "\" FROM \"" + mapping + "\"";

        String columnNames = columnMetadata.stream()
                .map(sqlColumnMetadata -> '\"' + sqlColumnMetadata.getName() + '\"')
                .collect(joining(", "));

        storeSink = String.format("SINK INTO \"%s\" (%s) VALUES (%s)", mapping, columnNames,
                queryParams(columnMetadata.size()));

        String setClause = columnMetadata.stream()
                .filter(cm -> !idColumn.equals(cm.getName()))
                .map(cm -> '\"' + cm.getName() + "\" = ?")
                .collect(joining(", "));

        storeUpdate = String.format("UPDATE \"%s\" SET %s WHERE \"%s\" = ?", mapping, setClause, idColumn);

        delete = String.format("DELETE FROM \"%s\" WHERE \"%s\" = ?", mapping, idColumn);

        deleteAllFactory = n -> String.format("DELETE FROM \"%s\" WHERE \"%s\" IN (%s)", mapping, idColumn,
                queryParams(n));
    }

    // Generate "?, ?" string for JDBC parameter binding
    private String queryParams(long n) {
        return Stream.generate(() -> "?").limit(n).collect(joining(", "));
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
}
