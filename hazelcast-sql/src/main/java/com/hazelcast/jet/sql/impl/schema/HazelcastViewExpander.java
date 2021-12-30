/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.HazelcastSqlToRelConverter;
import com.hazelcast.jet.sql.impl.parse.QueryConverter;
import com.hazelcast.jet.sql.impl.parse.QueryParser;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.apache.calcite.prepare.Prepare.CatalogReader;

/**
 * Hazelcast implementation of Calcite's {@link RelOptTable.ViewExpander}.
 */
public class HazelcastViewExpander implements RelOptTable.ViewExpander {
    private final QueryParser parser;
    private final RelOptCluster relOptCluster;
    private final SqlToRelConverter sqlToRelConverter;
    private final Deque<List<String>> expansionStack = new ArrayDeque<>();

    public HazelcastViewExpander(SqlValidator validator, CatalogReader catalogReader, RelOptCluster relOptCluster) {
        this.parser = new QueryParser((HazelcastSqlValidator) validator);
        this.relOptCluster = relOptCluster;
        this.sqlToRelConverter = new HazelcastSqlToRelConverter(
                this,
                validator,
                catalogReader,
                relOptCluster,
                StandardConvertletTable.INSTANCE,
                QueryConverter.CONFIG
        );
    }

    @Override
    public RelRoot expandView(
            RelDataType rowType,
            String queryString,
            List<String> schemaPath,
            @Nullable List<String> viewPath
    ) {
        if (expansionStack.contains(viewPath)) {
            throw QueryException.error("Cycle detected in view references");
        }
        expansionStack.push(viewPath);
        SqlNode sqlNode = parser.parse(queryString).getNode();
        final RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, true);
        expansionStack.pop();
        final RelRoot root2 = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));

        final RelBuilder relBuilder = QueryConverter.CONFIG.getRelBuilderFactory().create(relOptCluster, null);
        return root2.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    }
}
