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

import com.hazelcast.jet.sql.impl.OptimizerContext;
import com.hazelcast.jet.sql.impl.parse.QueryConvertResult;
import com.hazelcast.jet.sql.impl.parse.QueryParseResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.view.View;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;

/**
 * Table object to represent virtual (view) table.
 */
public class ViewTable extends Table {
    private final View view;
    private boolean isStream;

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
    protected List<TableField> initFields() {
        OptimizerContext context = OptimizerContext.getThreadContext();
        Deque<String> expansionStack = context.getViewExpansionStack();
        String viewPath = getSchemaName() + "." + view.name();
        if (expansionStack.contains(viewPath)) {
            throw QueryException.error("Cycle detected in view references");
        }
        expansionStack.push(viewPath);
        QueryParseResult parseResult = context.parse(view.query());
        isStream = parseResult.isInfiniteRows();
        final QueryConvertResult convertResult = context.convert(parseResult.getNode());
        expansionStack.pop();
        List<RelDataTypeField> fieldList = convertResult.getRel().getRowType().getFieldList();
        List<TableField> res = new ArrayList<>(fieldList.size());
        for (RelDataTypeField f : fieldList) {
            res.add(new TableField(f.getName(), toHazelcastType(f.getType()), false));
        }
        return res;
    }

    public String getViewQuery() {
        return view.query();
    }
}
