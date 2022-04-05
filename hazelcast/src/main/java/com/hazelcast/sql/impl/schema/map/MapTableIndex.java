/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Definition of an index of the IMap or ReplicatedMap.
 */
public class MapTableIndex {

    private final String name;
    private final IndexType type;
    private final int componentsCount;

    /** Ordinals of the fields in the owning table. */
    private final List<Integer> fieldOrdinals;

    /** Expected types of field converters. */
    private final List<QueryDataType> fieldConverterTypes;

    public MapTableIndex(
        String name,
        IndexType type,
        int componentsCount,
        List<Integer> fieldOrdinals,
        List<QueryDataType> fieldConverterTypes
    ) {
        this.name = name;
        this.type = type;
        this.componentsCount = componentsCount;
        this.fieldOrdinals = fieldOrdinals;
        this.fieldConverterTypes = fieldConverterTypes;
    }

    public String getName() {
        return name;
    }

    public IndexType getType() {
        return type;
    }

    public int getComponentsCount() {
        return componentsCount;
    }

    public List<Integer> getFieldOrdinals() {
        return fieldOrdinals;
    }

    public List<QueryDataType> getFieldConverterTypes() {
        return fieldConverterTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapTableIndex index = (MapTableIndex) o;

        return componentsCount == index.componentsCount
            && name.equals(index.name)
            && type == index.type
            && fieldOrdinals.equals(index.fieldOrdinals)
            && fieldConverterTypes.equals(index.fieldConverterTypes);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + componentsCount;
        result = 31 * result + fieldOrdinals.hashCode();
        result = 31 * result + fieldConverterTypes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MapTableIndex {name='" + name + '\'' + ", type=" + type + ", componentsCount=" + componentsCount
            + ", fieldOrdinals=" + fieldOrdinals + ", fieldConverterTypes=" + fieldConverterTypes + '}';
    }
}
