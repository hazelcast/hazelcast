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
