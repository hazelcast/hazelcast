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

package com.hazelcast.sql.impl.schema;

import java.util.Collection;
import java.util.List;

/**
 * Generic interface that resolves tables belonging to a particular backend.
 * <p>
 * At the moment the interface does exactly what we need - provides tables and registers default search paths.
 * In future, if we have more objects to expose, it might be expanded or reworked completely.
 */
public interface TableResolver {
    /**
     * @return Search paths to be added for object resolution.
     */
    List<List<String>> getDefaultSearchPaths();

    /**
     * @return Collection of tables to be registered.
     */
    Collection<Table> getTables();
}
