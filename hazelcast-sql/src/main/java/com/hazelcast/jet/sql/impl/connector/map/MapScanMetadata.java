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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * POJO that contains all specific information to scan a partitioned map by Jet processor.
 */
public class MapScanMetadata implements IdentifiedDataSerializable {

    protected String mapName;
    protected QueryTargetDescriptor keyDescriptor;
    protected QueryTargetDescriptor valueDescriptor;
    protected List<QueryPath> fieldPaths;
    protected List<QueryDataType> fieldTypes;
    protected List<Expression<?>> projections;
    protected Expression<Boolean> filter;

    public MapScanMetadata() {
        // No-op.
    }

    public MapScanMetadata(
            String mapName,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            List<QueryPath> fieldPaths,
            List<QueryDataType> fieldTypes,
            List<Expression<?>> projections,
            Expression<Boolean> filter
    ) {
        this.mapName = mapName;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.fieldPaths = fieldPaths;
        this.fieldTypes = fieldTypes;
        this.projections = projections;
        this.filter = filter;
    }

    public String getMapName() {
        return mapName;
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

    public List<Expression<?>> getProjects() {
        return projections;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapScanMetadata that = (MapScanMetadata) o;

        return mapName.equals(that.mapName)
                && keyDescriptor.equals(that.keyDescriptor)
                && valueDescriptor.equals(that.valueDescriptor)
                && fieldPaths.equals(that.fieldPaths)
                && fieldTypes.equals(that.fieldTypes)
                && projections.equals(that.projections)
                && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        int result = mapName.hashCode();
        result = 31 * result + keyDescriptor.hashCode();
        result = 31 * result + valueDescriptor.hashCode();
        result = 31 * result + fieldPaths.hashCode();
        result = 31 * result + fieldTypes.hashCode();
        result = 31 * result + projections.hashCode();
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldPaths=" + fieldPaths
                + ", projects=" + projections + ", filter=" + filter + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
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
        keyDescriptor = in.readObject();
        valueDescriptor = in.readObject();
        fieldPaths = SerializationUtil.readList(in);
        fieldTypes = SerializationUtil.readList(in);
        projections = SerializationUtil.readList(in);
        filter = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.IMAP_SCAN_METADATA;
    }
}
