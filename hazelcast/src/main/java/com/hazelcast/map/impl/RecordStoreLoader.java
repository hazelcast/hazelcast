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
        public void loadInitialKeys() {

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
     */
    void loadInitialKeys();

    /**
     * Picks and returns any one of throwables during load all process.
     * Returns null if there is no exception occurred.
     *
     * @return exception or null.
     */
    Throwable getExceptionOrNull();

}
