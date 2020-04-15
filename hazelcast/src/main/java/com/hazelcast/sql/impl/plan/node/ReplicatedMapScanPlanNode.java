/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Objects;

/**
 * Node to scan a replicated map.
 */
public class ReplicatedMapScanPlanNode extends AbstractMapScanPlanNode {
    public ReplicatedMapScanPlanNode() {
        // No-op.
    }

    public ReplicatedMapScanPlanNode(
        int id,
        String mapName,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<String> fieldNames,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter
    ) {
        super(id, mapName, keyDescriptor, valueDescriptor, fieldNames, fieldTypes, projects, filter);
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onReplicatedMapScanNode(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, mapName, fieldNames, projects, filter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractMapScanPlanNode that = (AbstractMapScanPlanNode) o;

        return id == that.id
            && mapName.equals(that.mapName)
            && fieldNames.equals(that.fieldNames)
            && projects.equals(that.projects)
            && Objects.equals(filter, that.filter);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", mapName=" + mapName + ", fieldNames=" + fieldNames
            + ", projects=" + projects + ", filter=" + filter + '}';
    }
}
