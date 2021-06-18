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

import com.hazelcast.function.ComparatorEx;
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
 * POJO that contains all specific information to scan a partitioned map index by Jet processor.
 */
public class MapIndexScanMetadata implements IdentifiedDataSerializable {

    protected String mapName;
    protected String indexName;
    protected QueryTargetDescriptor keyDescriptor;
    protected QueryTargetDescriptor valueDescriptor;
    protected List<QueryPath> fieldPaths;
    protected List<QueryDataType> fieldTypes;
    protected List<Expression<?>> projections;
    protected IndexFilter filter;
    protected ComparatorEx<Object[]> comparator;

    public MapIndexScanMetadata() {
        // No-op.
    }

    public MapIndexScanMetadata(
            String mapName,
            String indexName,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            List<QueryPath> fieldPaths,
            List<QueryDataType> fieldTypes,
            List<Expression<?>> projections,
            IndexFilter filter,
            ComparatorEx<Object[]> comparator
    ) {
        this.mapName = mapName;
        this.indexName = indexName;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.fieldPaths = fieldPaths;
        this.fieldTypes = fieldTypes;
        this.projections = projections;
        this.filter = filter;
        this.comparator = comparator;
    }

    public String getMapName() {
        return mapName;
    }

    public String getIndexName() {
        return indexName;
    }

    public QueryTargetDescriptor getKeyDescriptor() {
        return keyDescriptor;
    }

    public QueryTargetDescriptor getValueDescriptor() {
        return valueDescriptor;
    }

    public List<QueryPath> getFieldPaths() {
        return fieldPaths;
    }

    public List<QueryDataType> getFieldTypes() {
        return fieldTypes;
    }

    public List<Expression<?>> getProjections() {
        return projections;
    }

    public IndexFilter getFilter() {
        return filter;
    }

    public ComparatorEx<Object[]> getComparator() {
        return comparator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MapIndexScanMetadata that = (MapIndexScanMetadata) o;
        return mapName.equals(that.mapName)
                && indexName.equals(that.indexName)
                && keyDescriptor.equals(that.keyDescriptor)
                && valueDescriptor.equals(that.valueDescriptor)
                && fieldPaths.equals(that.fieldPaths)
                && fieldTypes.equals(that.fieldTypes)
                && projections.equals(that.projections)
                && filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapName, indexName, keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projections, filter);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeString(indexName);
        out.writeObject(keyDescriptor);
        out.writeObject(valueDescriptor);
        SerializationUtil.writeList(fieldPaths, out);
        SerializationUtil.writeList(fieldTypes, out);
        SerializationUtil.writeList(projections, out);
        out.writeObject(filter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        indexName = in.readString();
        keyDescriptor = in.readObject();
        valueDescriptor = in.readObject();
        fieldPaths = SerializationUtil.readList(in);
        fieldTypes = SerializationUtil.readList(in);
        projections = SerializationUtil.readList(in);
        filter = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.MAP_SCAN_METADATA;
    }
}
