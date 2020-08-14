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

package com.hazelcast.sql.impl.plan.cache;

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;

import java.util.List;
import java.util.Set;

public class PartitionedMapPlanObjectKey implements PlanObjectKey {

    private final String schemaName;
    private final String name;
    private final List<TableField> fields;
    private final QueryTargetDescriptor keyDescriptor;
    private final QueryTargetDescriptor valueDescriptor;
    private final List<MapTableIndex> indexes;
    private final int distributionFieldOrdinal;
    private final boolean hd;
    private final Set<String> conflictingSchemas;

    public PartitionedMapPlanObjectKey(
        String schemaName,
        String name,
        List<TableField> fields,
        Set<String> conflictingSchemas,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<MapTableIndex> indexes,
        int distributionFieldOrdinal,
        boolean hd
    ) {
        this.schemaName = schemaName;
        this.name = name;
        this.fields = fields;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.indexes = indexes;
        this.distributionFieldOrdinal = distributionFieldOrdinal;
        this.hd = hd;
        this.conflictingSchemas = conflictingSchemas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionedMapPlanObjectKey that = (PartitionedMapPlanObjectKey) o;

        return distributionFieldOrdinal == that.distributionFieldOrdinal
            && hd == that.hd
            && schemaName.equals(that.schemaName)
            && name.equals(that.name)
            && fields.equals(that.fields)
            && keyDescriptor.equals(that.keyDescriptor)
            && valueDescriptor.equals(that.valueDescriptor)
            && indexes.equals(that.indexes)
            && conflictingSchemas.equals(that.conflictingSchemas);
    }

    @Override
    public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + fields.hashCode();
        result = 31 * result + keyDescriptor.hashCode();
        result = 31 * result + valueDescriptor.hashCode();
        result = 31 * result + indexes.hashCode();
        result = 31 * result + distributionFieldOrdinal;
        result = 31 * result + (hd ? 1 : 0);
        result = 31 * result + conflictingSchemas.hashCode();
        return result;
    }
}
