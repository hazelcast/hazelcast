/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.annotation.Beta;

/**
 * Service for accessing external data store factories
 *
 * @since 5.2
 */
@Beta
public interface ExternalDataStoreService extends AutoCloseable {

    /**
     * Tests external data store configuration.
     *
     * @param config name of the data store factory
     * @return {@code true} if test was successful
     * @throws Exception if the test operation fails
     * @since 5.3
     */
    boolean testConnection(ExternalDataStoreConfig config) throws Exception;

    /**
     * Returns external data store factory with given name.
     *
     * @param name name of the data store factory
     * @param <DS> type of the data store
     * @return instance of the factory
     * @throws HazelcastException if the factory with given name is not found or misconfigured*
     */
    <DS> ExternalDataStoreFactory<DS> getExternalDataStoreFactory(String name);

    @Override
    void close();
}
