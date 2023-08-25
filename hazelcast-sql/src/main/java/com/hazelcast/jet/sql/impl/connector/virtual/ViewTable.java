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
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.isUnbounded;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;

/**
 * Table object to represent virtual (view) table.
 */
public class ViewTable extends Table {
    private final String viewQuery;
    private RelNode viewRel;

    public ViewTable(String schemaName, String viewName, String viewQuery, TableStatistics statistics) {
        // will determine if the view is streaming later, when it is used
        super(schemaName, viewName, null, statistics, "View", false);
        this.viewQuery = viewQuery;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new ViewPlanObjectKey(getSchemaName(), getSqlName(), viewQuery, getConflictingSchemas());
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
        context.getUsedViews().add(getObjectKey());
        streaming = isUnbounded(viewRel);
        return res;
    }

    public RelNode getViewRel() {
        getFields(); // called for the side effect of calling initFields()
        assert viewRel != null;
        return viewRel;
    }

    @Override
    public boolean isStreaming() {
        getFields(); // called for the side effect of calling initFields()
        return super.isStreaming();
    }

    static class ViewPlanObjectKey implements PlanObjectKey {
        private final String schemaName;
        private final String viewName;
        private final String query;
        private final Set<String> conflictingSchemas;

        ViewPlanObjectKey(
                String schemaName,
                String viewName,
                String query,
                Set<String> conflictingSchemas
        ) {
            this.schemaName = schemaName;
            this.viewName = viewName;
            this.query = query;
            this.conflictingSchemas = conflictingSchemas;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ViewPlanObjectKey that = (ViewPlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName)
                    && Objects.equals(viewName, that.viewName)
                    && Objects.equals(query, that.query)
                    && Objects.equals(conflictingSchemas, that.conflictingSchemas);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, viewName, query, conflictingSchemas);
        }
    }
}
