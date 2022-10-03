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

package com.hazelcast.datastore;

import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.spi.annotation.Beta;

/**
 * Basic implementation of the {@link ExternalDataStoreFactory}.
 * Requires implementing only how to create a datastore and how to close the factory
 *
 * @param <DS> - type of the data store
 * @since 5.2
 */
@Beta
public abstract class AbstractDataStoreFactory<DS> implements ExternalDataStoreFactory<DS> {

    protected DS sharedDataStore;
    protected ExternalDataStoreConfig config;

    /**
     * Creates a new data store
     *
     * @return a new data store instance
     */
    protected abstract DS createDataSource();

    @Override
    public void init(ExternalDataStoreConfig config) {
        this.config = config;
        if (config.isShared()) {
            sharedDataStore = createDataSource();
        }
    }

    @Override
    public DataStoreHolder<DS> createDataStore() {
        return config.isShared() ? DataStoreHolder.nonClosing(sharedDataStore) : DataStoreHolder.closing(createDataSource());
    }

}
