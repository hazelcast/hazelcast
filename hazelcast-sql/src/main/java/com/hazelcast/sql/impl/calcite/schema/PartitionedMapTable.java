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

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Statistic;

import java.util.List;
import java.util.Map;

/**
 * Table backed by an IMap.
 */
public class PartitionedMapTable extends AbstractMapTable {

    /** The field that is used for distributing keys for this table. */
    private final String distributionField;

    public PartitionedMapTable(
        String schemaName,
        String name,
        String distributionField,
        List<HazelcastTableIndex> indexes,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        Map<String, QueryDataType> fieldTypes,
        Map<String, String> fieldPaths,
        Statistic statistic
    ) {
        super(schemaName, name, indexes, keyDescriptor, valueDescriptor, fieldTypes, fieldPaths, statistic);

        this.distributionField = distributionField;
    }

    /**
     * @return Name of the distribution field or {@code null} if no distribution field is defined.
     */
    public String getDistributionField() {
        return distributionField;
    }
}
