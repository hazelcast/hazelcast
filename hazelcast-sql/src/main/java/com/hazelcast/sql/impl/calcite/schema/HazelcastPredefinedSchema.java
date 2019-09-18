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
import org.apache.calcite.schema.Table;

import java.util.Map;

/**
 * Schema operating on either partitioned or replicated maps.
 */
public class HazelcastPredefinedSchema extends HazelcastAbstractSchema {
    /** Name of partitioned schema. */
    private static final String SCHEMA_NAME_PARTITIONED = "partitioned";

    /** Name of replicated schema. */
    private static final String SCHEMA_NAME_REPLICATED = "replicated";

    /** Schema mode. */
    private final Mode mode;

    public HazelcastPredefinedSchema(NodeEngine nodeEngine, Mode mode) {
        super(nodeEngine);

        this.mode = mode;
    }

    @Override
    public Map<String, Table> getTableMap() {
        if (mode == Mode.PARTITIONED)
            return getPartitionedTables();
        else
            return getReplicatedTables();
    }

    /**
     * Schema mode.
     */
    public enum Mode {
        /** Partitioned maps. */
        PARTITIONED(SCHEMA_NAME_PARTITIONED),

        /** Replicated maps. */
        REPLICATED(SCHEMA_NAME_REPLICATED);

        /** Schema name associated with this mode. */
        private final String name;

        Mode(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }
    }
}
