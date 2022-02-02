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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
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
    protected List<Expression<?>> projection;
    protected Expression<Boolean> remainingFilter;
    protected IndexFilter filter;
    protected ComparatorEx<JetSqlRow> comparator;
    protected boolean descending;

    public MapIndexScanMetadata() {
        // No-op.
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public MapIndexScanMetadata(
            String mapName,
            String indexName,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            List<QueryPath> fieldPaths,
            List<QueryDataType> fieldTypes,
            IndexFilter filter,
            List<Expression<?>> projections,
            Expression<Boolean> remainingFilter,
            ComparatorEx<JetSqlRow> comparator,
            boolean descending
    ) {
        this.mapName = mapName;
        this.indexName = indexName;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.fieldPaths = fieldPaths;
        this.fieldTypes = fieldTypes;
        this.projection = projections;
        this.remainingFilter = remainingFilter;
        this.filter = filter;
        this.comparator = comparator;
        this.descending = descending;
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

    public List<Expression<?>> getProjection() {
        return projection;
    }

    public IndexFilter getFilter() {
        return filter;
    }

    public Expression<Boolean> getRemainingFilter() {
        return remainingFilter;
    }

    public ComparatorEx<JetSqlRow> getComparator() {
        return comparator;
    }

    public boolean isDescending() {
        return descending;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
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
                && projection.equals(that.projection)
                && Objects.equals(remainingFilter, that.remainingFilter)
                && Objects.equals(filter, that.filter)
                && Objects.equals(comparator, that.comparator)
                && descending == that.descending;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                mapName,
                indexName,
                keyDescriptor,
                valueDescriptor,
                fieldPaths,
                fieldTypes,
                projection,
                remainingFilter,
                filter,
                comparator,
                descending
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeString(indexName);
        out.writeObject(keyDescriptor);
        out.writeObject(valueDescriptor);
        SerializationUtil.writeList(fieldPaths, out);
        SerializationUtil.writeList(fieldTypes, out);
        SerializationUtil.writeList(projection, out);
        out.writeObject(filter);
        out.writeObject(remainingFilter);
        out.writeObject(comparator);
        out.writeBoolean(descending);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        indexName = in.readString();
        keyDescriptor = in.readObject();
        valueDescriptor = in.readObject();
        fieldPaths = SerializationUtil.readList(in);
        fieldTypes = SerializationUtil.readList(in);
        projection = SerializationUtil.readList(in);
        filter = in.readObject();
        remainingFilter = in.readObject();
        comparator = in.readObject();
        descending = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.MAP_INDEX_SCAN_METADATA;
    }
}
