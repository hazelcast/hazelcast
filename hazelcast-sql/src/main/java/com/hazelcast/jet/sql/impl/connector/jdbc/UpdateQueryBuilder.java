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

import com.google.common.primitives.Ints;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

class UpdateQueryBuilder {

    private final String query;
    private final List<Integer> parameterPositions = new ArrayList<>();

    UpdateQueryBuilder(JdbcTable table, List<String> pkFields, List<String> fieldNames, List<RexNode> expressions) {
        SqlDialect dialect = table.sqlDialect();

        assert fieldNames.size() == expressions.size();
        String setSqlFragment = Pair.zip(fieldNames, expressions).stream()
                .map(pair -> {
                    int pos;
                    if (pair.right instanceof RexInputRef) {
                        // a positive value is input reference
                        pos = ((RexInputRef) pair.right).getIndex();
                    } else if (pair.right instanceof RexDynamicParam) {
                        // a negative value minus one is dynamic param reference
                        pos = -((RexDynamicParam) pair.right).getIndex() - 1;
                    } else {
                        throw new UnsupportedOperationException(requireNonNull(pair.right).toString());
                    }
                    parameterPositions.add(pos);
                    String externalFieldName = table.getField(fieldNames.get(i)).externalName();
                    return dialect.quoteIdentifier(table.getField(pair.left).externalName()) + "=?";
                })
                .collect(joining(", "));

        String whereClause = IntStream.range(0, pkFields.size())
                .mapToObj(i -> {
                    parameterPositions.add(i);
                    return dialect.quoteIdentifier(pkFields.get(i)) + "=?";
                })
                .collect(joining(" AND "));

        query = "UPDATE " + dialect.quoteIdentifier(new StringBuilder(), Arrays.asList(table.getExternalName())) +
                " SET " + setSqlFragment +
                " WHERE " + whereClause;
    }

    String query() {
        return query;
    }

    int[] parameterPositions() {
        return Ints.toArray(parameterPositions);
    }
}
