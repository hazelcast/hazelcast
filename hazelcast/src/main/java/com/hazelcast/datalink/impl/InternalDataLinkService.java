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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.DataLinkService;

import java.util.Map;

/**
 * Service for managing {@link DataLink}s and their lifecycle.
 * <p>
 * {@link DataLink}s defined in the configuration are created at startup.
 * {@link DataLink}s added to the configuration dynamically are created via
 * {@link #createConfigDataLink(DataLinkConfig)}.
 * {@link DataLink}s created via SQL are added via {@link #replaceSqlDataLink(String, String, Map)},
 * these can be removed via {@link #removeDataLink(String)}.
 * <p>
 * When a new config is added via {@link #createConfigDataLink(DataLinkConfig)}
 * and SQL DataLink with the same name already exists, the SQL DataLink is
 * removed.
 */
public interface InternalDataLinkService extends DataLinkService {

    /**
     * Creates a new DataLink with the given config.
     * <p>
     * Such DataLink is considered persistent and can't be removed.
     * The DataLink is closed when {@link #shutdown()} is called.
     *
     * @param config the configuration of the DataLink
     */
    void createConfigDataLink(DataLinkConfig config);

    /**
     * Creates a new or replaces an existing DataLink with the given parameters.
     *
     * @param name      name of the DataLink
     * @param type      type of the DataLink
     * @param shared    is DataLink shared
     * @param options options configuring the DataLink
     */
    void replaceSqlDataLink(String name, String type, boolean shared, Map<String, String> options);

    /**
     * Returns if a {@link DataLink} with given name exists in the config
     *
     * @param name name of the DataLink
     * @return true if a {@link DataLink} exists in the config, false otherwise
     */
    boolean existsConfigDataLink(String name);

    /**
     * Returns if a {@link DataLink} with given name exists, created by SQL.
     *
     * @param name name of the DataLink
     * @return true if a {@link DataLink} exists, created by SQL; false otherwise
     */
    boolean existsSqlDataLink(String name);


    /**
     * Removes a DataLink.
     *
     * @param name name of the DataLink
     * @throws IllegalArgumentException if the DataLink was created through config,
     *                                  not via {@link #replaceSqlDataLink(String, String, boolean, Map)}
     */
    void removeDataLink(String name);

    /**
     * Return type of the DataLink with the given name
     * @param name name of the DataLink
     * @return type of the data link
     */
    String typeForDataLink(String name);

    /**
     * Return class implementing DataLink of given type.
     * @param type type of the DataLink
     * @return DataLink implementation class
     */
    Class<? extends DataLink> classForDataLinkType(String type);

    /**
     * Close this DataLinkService, should be called only on member shutdown.
     */
    void shutdown();
}
