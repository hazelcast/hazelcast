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
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * POJO that contains all specific information to scan map indexes by Jet processor.
 */
public class MapIndexScanMetadata extends MapScanMetadata {

    protected String indexName;
    protected int indexComponentCount;
    protected IndexFilter indexFilter;
    protected List<QueryDataType> converterTypes;
    protected List<Boolean> ascs;

    public MapIndexScanMetadata() {
        // No-op.
    }

    public MapIndexScanMetadata(
            MapScanMetadata scanMetadata,
            String indexName,
            int indexComponentCount,
            IndexFilter indexFilter,
            List<QueryDataType> converterTypes,
            List<Boolean> ascs
    ) {
        super(
                scanMetadata.getMapName(),
                scanMetadata.getKeyDescriptor(),
                scanMetadata.getValueDescriptor(),
                scanMetadata.getFieldPaths(),
                scanMetadata.getFieldTypes(),
                scanMetadata.getProjects(),
                scanMetadata.getFilter()
        );

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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapIndexScanMetadata that = (MapIndexScanMetadata) o;

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
        super.writeData(out);

        out.writeString(indexName);
        out.writeInt(indexComponentCount);
        out.writeObject(indexFilter);
        SerializationUtil.writeList(converterTypes, out);
        SerializationUtil.writeList(ascs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        indexName = in.readString();
        indexComponentCount = in.readInt();
        indexFilter = in.readObject();
        converterTypes = SerializationUtil.readList(in);
        ascs = SerializationUtil.readList(in);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.MAP_INDEX_SCAN_METADATA;
    }
}
