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

    private final String storeInsert;
    private final String storeUpdate;
    private final String delete;

    private final Function<Integer, String> deleteAllFactory;
    private final Map<Integer, String> deleteAllQueries = new ConcurrentHashMap<>();

    Queries(String mapping, String idColumn, List<SqlColumnMetadata> columnMetadata) {
        loadQuery = "SELECT * FROM \"" + mapping + "\" WHERE \"" + idColumn + "\" = ?";

        loadAllFactory = n -> "SELECT * FROM \"" + mapping + "\" WHERE \"" + idColumn + "\" IN ("
                + queryParams(n)
                + ")";

        loadAllKeys = "SELECT \"" + idColumn + "\" FROM \"" + mapping + "\"";

        String columnNames = columnMetadata.stream()
                                           .map(sqlColumnMetadata -> '\"' + sqlColumnMetadata.getName() + '\"')
                                           .collect(joining(", "));

        storeInsert = "INSERT INTO \"" + mapping + "\" ( " + columnNames + " ) VALUES (" +
                queryParams(columnMetadata.size()) +
                ")";

        String setClause = columnMetadata.stream()
                                         .filter(cm -> !idColumn.equals(cm.getName()))
                                         .map(cm -> '\"' + cm.getName() + "\" = ?")
                                         .collect(joining(", "));

        storeUpdate = "UPDATE \"" + mapping + "\" SET " + setClause
                + " WHERE \"" + idColumn + "\" = ?";

        delete = "DELETE FROM \"" + mapping + "\" WHERE \"" + idColumn + "\" = ?";
        deleteAllFactory = n -> "DELETE FROM \"" + mapping + "\" WHERE \"" + idColumn + "\" IN ("
                + queryParams(n)
                + ")";
    }

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

    String storeInsert() {
        return storeInsert;
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
