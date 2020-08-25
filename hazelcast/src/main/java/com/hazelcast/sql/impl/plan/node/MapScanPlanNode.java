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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Node to scan a partitioned map.
 */
public class MapScanPlanNode extends AbstractMapScanPlanNode implements IdentifiedDataSerializable {
    public MapScanPlanNode() {
        // No-op.
    }

    public MapScanPlanNode(
        int id,
        String mapName,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter
    ) {
        super(id, mapName, keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projects, filter);
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onMapScanNode(this);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_MAP_SCAN;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", mapName=" + mapName + ", fieldPaths=" + fieldPaths
            + ", projects=" + projects + ", filter=" + filter + '}';
    }
}
