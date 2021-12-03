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
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.view.View;

import java.util.List;

/**
 * Table object to represent virtual (view) table.
 */
public class ViewTable extends Table {
    private final View view;

    public ViewTable(
            String schemaName,
            View view,
            List<TableField> fields,
            TableStatistics statistics
    ) {
        super(schemaName, view.name(), fields, statistics);
        this.view = view;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        // views never participate in plans, they are expanded
        throw new UnsupportedOperationException("Never should be called");
    }

    public String getViewQuery() {
        return view.query();
    }
}
