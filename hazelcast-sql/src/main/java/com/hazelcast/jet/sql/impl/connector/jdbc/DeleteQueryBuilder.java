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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import java.util.ArrayList;
import java.util.List;

class DeleteQueryBuilder extends AbstractQueryBuilder {

    private final List<Integer> dynamicParams = new ArrayList<>();
    private final List<Integer> inputRefs = new ArrayList<>();

    DeleteQueryBuilder(
            JdbcTable table,
            SqlDialect dialect,
            RexNode predicate,
            boolean hasInput
    ) {
        super(table, dialect);

        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        dialect.quoteIdentifier(sb, table.getExternalNameList());

        if (predicate != null) {
            appendPredicate(sb, predicate, dynamicParams);
        } else if (hasInput) {
            appendPrimaryKeyPredicate(sb, inputRefs);
        }

        query = sb.toString();
    }

    int[] dynamicParams() {
        return Ints.toArray(dynamicParams);
    }

    int[] inputRefs() {
        return Ints.toArray(inputRefs);
    }
}
