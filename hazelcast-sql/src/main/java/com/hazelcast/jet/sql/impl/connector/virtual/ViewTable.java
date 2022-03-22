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

package com.hazelcast.jet.sql.impl.connector.virtual;

import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.view.View;

/**
 * Table object to represent virtual (view) table.
 */
public class ViewTable extends Table {
    private final View view;

    public ViewTable(String schemaName, View view, TableStatistics statistics) {
        super(schemaName, view.name(), null, statistics);
        this.view = view;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        // Views never participate in plans, they are expanded. We return non-cacheable - if
        // for any reason a plan contains a view, the plan wouldn't be cached.
        return PlanObjectKey.NON_CACHEABLE_OBJECT_KEY;
    }

    @Override
    protected void initFields() {
//        if (expansionStack.contains(viewPath)) {
//            throw QueryException.error("Cycle detected in view references");
//        }
//        expansionStack.push(viewPath);
//        SqlNode sqlNode = parser.parse(queryString).getNode();
//        final RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, true);
//        expansionStack.pop();
//        final RelRoot root2 = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
//
//        final RelBuilder relBuilder = QueryConverter.CONFIG.getRelBuilderFactory().create(relOptCluster, null);
//        return root2.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        throw new UnsupportedOperationException("TODO");
    }

    public String getViewQuery() {
        return view.query();
    }

    @Override
    public boolean isStream() {
        // TODO [viliam]
        return false;
    }
}
