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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.sql.impl.type.DataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Hazelcast table which can register fields dynamically.
 */
public class HazelcastTable extends AbstractTable {
    /** Schema name. */
    private final String schemaName;

    /** Name. */
    private final String name;

    /** Whether this is a partitioned map. */
    private final boolean partitioned;

    /** Distribution field name. */
    private final String distributionField;

    /** Indexes. */
    private final List<HazelcastTableIndex> indexes;

    /** Table statistic. */
    private final Statistic statistic;

    /** Field types. */
    private final Map<String, DataType> fieldTypes;

    /** Field paths. */
    private final Map<String, String> fieldPaths;

    /** Fields. */
    // TODO: We used this object for dynamic type resolution. With static schema we do not need it anymore. Refactor.
    private final HazelcastTableFields fields = new HazelcastTableFields();

    public HazelcastTable(
        String schemaName,
        String name,
        boolean partitioned,
        String distributionField,
        List<HazelcastTableIndex> indexes,
        Map<String, DataType> fieldTypes,
        Map<String, String> fieldPaths,
        Statistic statistic
    ) {
        this.schemaName = schemaName;
        this.name = name;
        this.partitioned = partitioned;
        this.distributionField = distributionField;
        this.fieldTypes = fieldTypes != null ? fieldTypes : Collections.emptyMap();
        this.fieldPaths = fieldPaths != null ? fieldPaths : Collections.emptyMap();
        this.indexes = indexes != null ? indexes : Collections.emptyList();
        this.statistic = statistic;
    }

    public DataType getFieldType(String fieldName) {
        DataType fieldType = fieldTypes.get(fieldName);

        if (fieldType == null) {
            fieldType = DataType.LATE;
        }

        return fieldType;
    }

    public String getFieldPath(String fieldName) {
        String path = fieldPaths.get(fieldName);

        return path != null ? path : fieldName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getName() {
        return name;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public boolean isReplicated() {
        return !isPartitioned();
    }

    public String getDistributionField() {
        return distributionField;
    }

    public List<HazelcastTableIndex> getIndexes() {
        return indexes;
    }

    public List<RelDataTypeField> getFieldList() {
        return fields.getFieldList();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new HazelcastTableRelDataType(typeFactory, fields);
    }

    @Override
    public final Statistic getStatistic() {
        return statistic;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{name=" + name + ", partitioned=" + partitioned
            + ", distributionField=" + distributionField + ", indexes=" + indexes + '}';
    }
}
