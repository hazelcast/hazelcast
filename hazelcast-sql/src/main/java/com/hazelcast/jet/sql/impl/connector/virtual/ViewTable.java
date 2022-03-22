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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;

/**
 * Table object to represent virtual (view) table.
 */
public class ViewTable extends Table {
    private final String viewQuery;
    private RelNode viewRel;

    public ViewTable(String schemaName, String viewName, String viewQuery, TableStatistics statistics) {
        super(schemaName, viewName, null, statistics);
        this.viewQuery = viewQuery;
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
        String viewPath = getSchemaName() + "." + getSqlName();
        if (expansionStack.contains(viewPath)) {
            throw QueryException.error("Cycle detected in view references");
        }
        expansionStack.push(viewPath);
        SqlNode sqlNode = context.parse(viewQuery).getNode();
        viewRel = context.convertView(sqlNode);
        expansionStack.pop();
        List<RelDataTypeField> fieldList = viewRel.getRowType().getFieldList();
        List<TableField> res = new ArrayList<>(fieldList.size());
        for (RelDataTypeField f : fieldList) {
            res.add(new TableField(f.getName(), toHazelcastType(f.getType()), false));
        }
        return res;
    }

    public RelNode getViewRel() {
        getFields(); // called for the side effect of calling initFields()
        assert viewRel != null;
        return viewRel;
    }
}
