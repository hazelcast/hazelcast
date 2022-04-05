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

package com.hazelcast.sql.impl.schema;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Generic interface that resolves tables belonging to a particular backend.
 * <p>
 * At the moment the interface does exactly what we need - provides tables and registers default search paths.
 * In the future, if we have more objects to expose, it might be expanded or reworked completely.
 */
public interface TableResolver {
    /**
     * Gets the list of search paths for object resolution.
     * <p>
     * A single search path consists of two elements: predefined catalog (see {@link com.hazelcast.sql.impl.QueryUtils#CATALOG}
     * and schema name. For example {@code {"hazelcast", "schema"}}. In this case the "schema" will be added to a search paths,
     * so that a table {@code schema.table} could be referred as {@code table} in SQL scripts: {@code SELECT * FROM table}.
     * <p>
     * Order of search paths is important. If several search paths are defined, then the first path will be searched first, etc.
     * For example if a table with the same name is defined in {@code schema1} and {@code schema2}, and the following search
     * paths are provided {@code {"hazelcast", "schema1"}, {"hazelcast", "schema2"}}, then {@code SELECT * FROM table} will pick
     * the table from the {@code schema1}.
     *
     * @return The list of search paths for object resolution.
     */
    @Nonnull
    List<List<String>> getDefaultSearchPaths();

    /**
     * @return Collection of tables to be registered.
     */
    @Nonnull
    List<Table> getTables();

    /**
     * Adds a listener to be called when a {@see Table} is added, removed or changed.
     */
    void registerListener(TableListener listener);

    /**
     * Listeners interested in acting upon {@see Table} changes should implement this interface.
     */
    interface TableListener {

        /**
         * Invoked on registered listeners after update/removal of a {@see Table}.
         */
        void onTableChanged();
    }
}
