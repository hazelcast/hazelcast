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

package com.hazelcast.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.annotation.Beta;

/**
 * Service for accessing data link factories
 *
 * @since 5.2
 */
@Beta
public interface DataLinkService extends AutoCloseable {

    /**
     * Tests data link configuration.
     *
     * @param config name of the data link factory
     * @return {@code true} if test was successful
     * @throws Exception if the test operation fails
     * @since 5.3
     */
    boolean testConnection(DataLinkConfig config) throws Exception;

    /**
     * Returns data link factory with given name.
     *
     * @param name name of the data link factory
     * @param <DL> type of the data link
     * @return instance of the factory
     * @throws HazelcastException if the factory with given name is not found or misconfigured*
     */
    <DL> DataLinkFactory<DL> getDataLinkFactory(String name);

    @Override
    void close();
}
