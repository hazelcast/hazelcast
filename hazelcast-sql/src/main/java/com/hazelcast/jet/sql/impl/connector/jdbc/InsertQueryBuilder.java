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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import java.util.List;

import static java.util.stream.Collectors.joining;

class InsertQueryBuilder {

    private final String query;

    InsertQueryBuilder(String tableName, List<String> fieldNames) {
        StringBuilder sb = new StringBuilder()
                .append("INSERT INTO ")
                .append(tableName)
                .append(" ( ")
                .append(fieldNames.stream().map(name -> "\"" + name + "\"").collect(joining(",")))
                .append(" ) ")
                .append(" VALUES (");
        for (int i = 0; i < fieldNames.size(); i++) {
            sb.append('?');
            if (i < (fieldNames.size() - 1)) {
                sb.append(", ");
            }
        }
        sb.append(')');
        query = sb.toString();
    }

    String query() {
        return query;
    }
}
