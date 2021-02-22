/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Node to scan a partitioned map.
 */
public class MapIndexScanPlanNode extends AbstractMapScanPlanNode implements IdentifiedDataSerializable {

    private String indexName;
    private int indexComponentCount;
    private IndexFilter indexFilter;
    private List<QueryDataType> converterTypes;
    private List<Boolean> ascs;

    public MapIndexScanPlanNode() {
        // No-op.
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public MapIndexScanPlanNode(
        int id,
        String mapName,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        String indexName,
        int indexComponentCount,
        IndexFilter indexFilter,
        List<QueryDataType> converterTypes,
        Expression<Boolean> remainderFilter,
        List<Boolean> ascs
    ) {
        super(id, mapName, keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projects, remainderFilter);

        this.indexName = indexName;
        this.indexComponentCount = indexComponentCount;
        this.indexFilter = indexFilter;
        this.converterTypes = converterTypes;
        this.ascs = ascs;
    }

    public String getIndexName() {
        return indexName;
    }

    public int getIndexComponentCount() {
        return indexComponentCount;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public List<QueryDataType> getConverterTypes() {
        return converterTypes;
    }

    public List<Boolean> getAscs() {
        return ascs;
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onMapIndexScanNode(this);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_MAP_INDEX_SCAN;
    }

    @Override
    protected void writeData0(ObjectDataOutput out) throws IOException {
        super.writeData0(out);

        out.writeString(indexName);
        out.writeInt(indexComponentCount);
        out.writeObject(indexFilter);
        SerializationUtil.writeList(converterTypes, out);
        SerializationUtil.writeList(ascs, out);
    }

    @Override
    protected void readData0(ObjectDataInput in) throws IOException {
        super.readData0(in);

        indexName = in.readString();
        indexComponentCount = in.readInt();
        indexFilter = in.readObject();
        converterTypes = SerializationUtil.readList(in);
        ascs = SerializationUtil.readList(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        MapIndexScanPlanNode that = (MapIndexScanPlanNode) o;

        return indexComponentCount == that.indexComponentCount
            && indexName.equals(that.indexName)
            && Objects.equals(indexFilter, that.indexFilter)
            && Objects.equals(converterTypes, that.converterTypes)
            && Objects.equals(ascs, that.ascs);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + indexName.hashCode();
        result = 31 * result + indexComponentCount;
        result = 31 * result + (indexFilter != null ? indexFilter.hashCode() : 0);
        result = 31 * result + (converterTypes != null ? converterTypes.hashCode() : 0);
        result = 31 * result + ascs.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", mapName=" + mapName + ", fieldPaths=" + fieldPaths
            + ", projects=" + projects + ", indexName=" + indexName + ", indexFilter=" + indexFilter
            + ", remainderFilter=" + filter
            + ", ascs=" + ascs + '}';
    }
}
