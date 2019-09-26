/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.NodeEngine;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.calcite.schema.HazelcastPredefinedSchema.Mode.PARTITIONED;
import static com.hazelcast.sql.impl.calcite.schema.HazelcastPredefinedSchema.Mode.REPLICATED;

/**
 * Root schema.
 */
public class HazelcastRootSchema extends HazelcastAbstractSchema {
    /** Child schemas. */
    private Map<String, Schema> subSchemaMap;

    public HazelcastRootSchema(NodeEngine nodeEngine) {
        super(nodeEngine);

        subSchemaMap = new HashMap<>();

        subSchemaMap.put(PARTITIONED.getName(), new HazelcastPredefinedSchema(nodeEngine, PARTITIONED));
        subSchemaMap.put(REPLICATED.getName(), new HazelcastPredefinedSchema(nodeEngine, REPLICATED));
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return subSchemaMap;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> replicatedTables = getReplicatedTables();
        Map<String, Table> partitionedTables = getPartitionedTables();

        Map<String, Table> res = new HashMap<>();

        for (Map.Entry<String, Table> table : replicatedTables.entrySet()) {
            res.put(table.getKey(), table.getValue());
        }

        for (Map.Entry<String, Table> table : partitionedTables.entrySet()) {
            res.put(table.getKey(), table.getValue());
        }

        return res;
    }
}
