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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.MapLoader;
import com.hazelcast.internal.serialization.Data;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Loader contract for a {@link RecordStore}.
 */
interface RecordStoreLoader {
    /**
     * Implementation of the {@link RecordStoreLoader} used
     * when the map store is not configured for a map or the
     * map store is disabled.
     */
    RecordStoreLoader EMPTY_LOADER = (keys, replaceExistingValues) -> null;

    /**
     * Triggers loading values for the given {@code keys} from the
     * defined {@link MapLoader}.
     * The values will be loaded asynchronously and this method will
     * return as soon as the value loading task has been offloaded
     * to a different thread.
     *
     * @param keys                  the keys for which values will be loaded
     * @param replaceExistingValues if the existing entries for the keys should
     *                              be replaced with the loaded values
     * @return future representing the pending completion for the value loading task
     */
    Future<?> loadValues(List<Data> keys, boolean replaceExistingValues);
}
