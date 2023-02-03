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

class DeleteQueryBuilder {

    private final String query;

    DeleteQueryBuilder(String tableName, List<String> pkFields) {

        String whereClause = pkFields.stream().map(e -> '\"' + e + "\" = ?")
                                     .collect(joining(" AND "));

        query = "DELETE FROM " + tableName +
                " WHERE " + whereClause;
    }

    String query() {
        return query;
    }
}
