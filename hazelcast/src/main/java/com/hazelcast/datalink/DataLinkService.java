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

import java.util.Map;

/**
 * Service for managing {@link DataLink}s and their lifecycle.
 * <p>
 * {@link DataLink}s defined in the configuration are created at startup.
 * {@link DataLink}s added to the configuration dynamically are created via
 * {@link #createConfigDataLink(DataLinkConfig)}.
 * {@link DataLink}s created via SQL are added via {@link #createSqlDataLink(String, String, Map)},
 * these can be removed via {@link #removeDataLink(String)}.
 * <p>
 * When a new config is added via {@link #createConfigDataLink(DataLinkConfig)} is created and SQL DataLink already
 * exists the existing DataLink is replaced and closed.
 *
 * @since 5.3
 */
@Beta
public interface DataLinkService extends AutoCloseable {

    /**
     * Creates a new DataLink with the given config
     * <p>
     * Such DataLink is considered immutable and can't be updated.
     * The DataLink is closed when {@link #close()} is called.
     *
     * @param config the configuration of the DataLink
     */
    void createConfigDataLink(DataLinkConfig config);

    /**
     * Creates a new DataLink with the given parameters.
     *
     * @param name    name of the DataLink
     * @param type    type of the DataLink
     * @param options options configuring the DataLink
     */
    void createSqlDataLink(String name, String type, Map<String, String> options);

    /**
     * Replaces an existing DataLink with a new one with given parameters.
     * <p>
     * The old DataLink is closed.
     *
     * @param name    name of the DataLink
     * @param type    type of the DataLink
     * @param options options configuring the DataLink
     */
    void replaceSqlDataLink(String name, String type, Map<String, String> options);

    /**
     * Returns DataLink with given name.
     *
     * @param name name of the DataLink
     * @param <T>  type of the DataLink
     * @return instance of the DataLink
     * @throws HazelcastException if the DataLink with given name is not found or misconfigured*
     */
    <T extends DataLink> T getDataLink(String name);

    /**
     * Returns DataLink with given name.
     * <p>
     * Type checked against the provided clazz parameter.
     *
     * @param name  name of the DataLink
     * @param clazz type of the DataLink
     */
    <T extends DataLink> T getDataLink(String name, Class<T> clazz);

    /**
     * Returns existence status of given data link name.
     *
     * @param name name of the DataLink
     */
    boolean dataLinkExists(String name);

    /**
     * Removes DataLink created by {@link #createSqlDataLink(String, String, Map)}
     * <p>
     * Removed DataLink is closed.
     *
     * @param name name of the DataLink
     * @throws IllegalArgumentException if the DataLink is defined in the config,
     *                                  not created via {@link #createSqlDataLink(String, String, Map)}
     */
    void removeDataLink(String name);

    /**
     * Close this DataLinkService, should be called only on member shutdown.
     */
    @Override
    void close();
}
