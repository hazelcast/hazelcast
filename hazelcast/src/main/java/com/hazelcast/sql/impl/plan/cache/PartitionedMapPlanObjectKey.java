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
import com.hazelcast.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.Set;

public class PartitionedMapPlanObjectKey implements PlanObjectKey {

    private final String schemaName;
    private final String name;
    private final List<TableField> fields;
    private final QueryTargetDescriptor keyQueryDescriptor;
    private final QueryTargetDescriptor valueQueryDescriptor;
    private final UpsertTargetDescriptor keyUpsertDescriptor;
    private final UpsertTargetDescriptor valueUpsertDescriptor;
    private final Set<String> conflictingSchemas;

    public PartitionedMapPlanObjectKey(
        String schemaName,
        String name,
        List<TableField> fields,
        QueryTargetDescriptor keyQueryDescriptor,
        QueryTargetDescriptor valueQueryDescriptor,
        UpsertTargetDescriptor keyUpsertDescriptor,
        UpsertTargetDescriptor valueUpsertDescriptor,
        Set<String> conflictingSchemas
    ) {
        this.schemaName = schemaName;
        this.name = name;
        this.fields = fields;
        this.keyQueryDescriptor = keyQueryDescriptor;
        this.valueQueryDescriptor = valueQueryDescriptor;
        this.keyUpsertDescriptor = keyUpsertDescriptor;
        this.valueUpsertDescriptor = valueUpsertDescriptor;
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

        return schemaName.equals(that.schemaName)
                && name.equals(that.name)
                && fields.equals(that.fields)
                && keyQueryDescriptor.equals(that.keyQueryDescriptor)
                && valueQueryDescriptor.equals(that.valueQueryDescriptor)
                && keyUpsertDescriptor.equals(that.keyUpsertDescriptor)
                && valueUpsertDescriptor.equals(that.valueUpsertDescriptor)
                && conflictingSchemas.equals(that.conflictingSchemas);
    }

    @Override
    public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + fields.hashCode();
        result = 31 * result + keyQueryDescriptor.hashCode();
        result = 31 * result + valueQueryDescriptor.hashCode();
        result = 31 * result + keyUpsertDescriptor.hashCode();
        result = 31 * result + valueUpsertDescriptor.hashCode();
        result = 31 * result + conflictingSchemas.hashCode();
        return result;
    }
}
