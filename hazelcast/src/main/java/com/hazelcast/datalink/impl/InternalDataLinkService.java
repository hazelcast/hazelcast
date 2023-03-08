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
 * {@link DataLink}s created via SQL are added via {@link #createSqlDataLink(String, String, Map, boolean)},
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
     * Creates a new DataLink with the given parameters.
     *
     * @param name    name of the DataLink
     * @param type    type of the DataLink
     * @param options options configuring the DataLink
     * @param replace if true, an existing data link with the same name is removed first.
     *                If false, if a DataLink with the same name exists, and error is thrown.
     */
    void createSqlDataLink(String name, String type, Map<String, String> options, boolean replace);

    /**
     * Returns if a DataLink with given name exists or not
     *
     * @param name name of the DataLink
     * @return true if a DataLink exists, false otherwise
     */
    boolean existsDataLink(String name);

    /**
     * Removes a DataLink.
     *
     * @param name name of the DataLink
     * @throws IllegalArgumentException if the DataLink was created through config,
     *                                  not via {@link #createSqlDataLink(String, String, Map, boolean)}
     */
    void removeDataLink(String name);

    /**
     * Return type of the DataLink with the given name
     * @param name name of the DataLink
     * @return type of the data link
     */
    String typeForDataLink(String name);

    /**
     * Close this DataLinkService, should be called only on member shutdown.
     */
    void shutdown();
}
