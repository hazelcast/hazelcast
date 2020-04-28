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

import org.apache.calcite.schema.impl.AbstractTable;

/**
 * Base class for all tables in the Calcite integration. Exposes table and schema names in order to form the complete
 * schema for optimization.
 */
public abstract class HazelcastAbstractTable extends AbstractTable {
    /**
     * Get name of the schema where the table resides. Must be globally unique.
     *
     * @return Name of the schema.
     */
    public abstract String getSchemaName();

    /**
     * Get name of the table. Must be unique within the schema.
     *
     * @return Name of the table.
     */
    public abstract String getName();

    /**
     * Whether the table should be included into the top schema.
     * <p>
     * When enabled, the table will be accessible via name without schema, as if it was located in the default schema.
     * Consider the table {@code partitioned.myMap} with this option enabled. This table could be accessed via both
     * {@code partitioned.myMap} and {@code myMap}.
     * <p>
     * This option should be enabled only for Hazelcast sources to provide ease of use - IMap and ReplicatedMap.
     * <p>
     * In case of the name conflict, the first registered object wins.
     *
     * @return {@code true} if the table should be included into the top schema.
     */
    public abstract boolean isTopLevel();
}
