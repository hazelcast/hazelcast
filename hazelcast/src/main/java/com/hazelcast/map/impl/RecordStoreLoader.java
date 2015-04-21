/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Loader contract for a {@link RecordStore}.
 */
interface RecordStoreLoader {

    RecordStoreLoader EMPTY_LOADER = new RecordStoreLoader() {
        @Override
        public Future loadValues(List<Data> keys, boolean replaceExistingValues) {
            return null;
        }
    };

    /**
     * Loads all keys from defined map store.
     *
     * @param keys                  keys to be loaded.
     * @param replaceExistingValues <code>true</code> if need to replace existing values otherwise <code>false</code>
     * @return future for checking when loading is complete
     */
    Future<?> loadValues(List<Data> keys, boolean replaceExistingValues);
}
