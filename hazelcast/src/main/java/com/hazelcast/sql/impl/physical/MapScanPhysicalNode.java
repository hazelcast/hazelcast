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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;
import com.hazelcast.sql.impl.type.DataType;

import java.util.List;
import java.util.Objects;

/**
 * Node to scan a partitioned map.
 */
public class MapScanPhysicalNode extends AbstractMapScanPhysicalNode {
    public MapScanPhysicalNode() {
        // No-op.
    }

    public MapScanPhysicalNode(
        int id,
        String mapName,
        List<String> fieldNames,
        List<DataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter
    ) {
        super(id, mapName, fieldNames, fieldTypes, projects, filter);
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onMapScanNode(this);
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

        AbstractMapScanPhysicalNode that = (AbstractMapScanPhysicalNode) o;

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
