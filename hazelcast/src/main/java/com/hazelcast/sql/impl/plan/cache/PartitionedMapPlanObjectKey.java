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

package com.hazelcast.sql.impl.plan.cache;

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class PartitionedMapPlanObjectKey implements PlanObjectKey {

    private final String schemaName;
    private final String tableName;
    private final String mapName;
    private final List<TableField> fields;
    private final QueryTargetDescriptor keyDescriptor;
    private final QueryTargetDescriptor valueDescriptor;
    private final Object keyJetMetadata;
    private final Object valueJetMetadata;
    private final List<MapTableIndex> indexes;
    private final boolean hd;
    private final Set<String> conflictingSchemas;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public PartitionedMapPlanObjectKey(
        String schemaName,
        String tableName,
        String mapName,
        List<TableField> fields,
        Set<String> conflictingSchemas,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        Object keyJetMetadata,
        Object valueJetMetadata,
        List<MapTableIndex> indexes,
        boolean hd
    ) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.mapName = mapName;
        this.fields = fields;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.keyJetMetadata = keyJetMetadata;
        this.valueJetMetadata = valueJetMetadata;
        this.indexes = indexes;
        this.hd = hd;
        this.conflictingSchemas = conflictingSchemas;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionedMapPlanObjectKey that = (PartitionedMapPlanObjectKey) o;

        return hd == that.hd
            && schemaName.equals(that.schemaName)
            && tableName.equals(that.tableName)
            && mapName.equals(that.mapName)
            && fields.equals(that.fields)
            && keyDescriptor.equals(that.keyDescriptor)
            && valueDescriptor.equals(that.valueDescriptor)
            && Objects.equals(keyJetMetadata, that.keyJetMetadata)
            && Objects.equals(valueJetMetadata, that.valueJetMetadata)
            && indexes.equals(that.indexes)
            && conflictingSchemas.equals(that.conflictingSchemas);
    }

    @Override
    public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + mapName.hashCode();
        result = 31 * result + fields.hashCode();
        result = 31 * result + keyDescriptor.hashCode();
        result = 31 * result + valueDescriptor.hashCode();
        result = 31 * result + Objects.hashCode(keyJetMetadata);
        result = 31 * result + Objects.hashCode(valueJetMetadata);
        result = 31 * result + indexes.hashCode();
        result = 31 * result + (hd ? 1 : 0);
        result = 31 * result + conflictingSchemas.hashCode();
        return result;
    }
}
