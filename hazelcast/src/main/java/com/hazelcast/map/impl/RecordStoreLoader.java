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

/**
 * Loader contract for a {@link RecordStore}.
 */
interface RecordStoreLoader {

    RecordStoreLoader EMPTY_LOADER = new RecordStoreLoader() {

        @Override
        public boolean isLoaded() {
            // return true when there is no map store.
            return true;
        }

        @Override
        public void setLoaded(boolean loaded) {

        }

        @Override
        public void loadAll(List<Data> keys, boolean replaceExistingValues) {

        }

        @Override
        public void loadInitialKeys(boolean replaceExisting) {

        }

        @Override
        public Throwable getExceptionOrNull() {
            return null;
        }
    };

    /**
     * Query whether load operation finished or not for a particular {@link RecordStore}
     *
     * @return <code>true</code> if load finished successfully, <code>false</code> otherwise.
     */
    boolean isLoaded();

    void setLoaded(boolean loaded);


    /**
     * Loads all keys from defined map store.
     *
     * @param keys                  keys to be loaded.
     * @param replaceExistingValues <code>true</code> if need to replace existing values otherwise <code>false</code>
     */
    void loadAll(List<Data> keys, boolean replaceExistingValues);

    /**
     * Loads initial keys.
     * @param replaceExisting keys
     */
    void loadInitialKeys(boolean replaceExisting);

    /**
     * Picks and returns any one of throwables during load all process.
     * Returns null if there is no exception occurred.
     *
     * @return exception or null.
     */
    Throwable getExceptionOrNull();

}
