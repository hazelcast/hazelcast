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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionService;

import java.util.Map;

/**
 * Service for managing {@link DataConnection}s and their lifecycle.
 * <p>
 * {@link DataConnection}s defined in the configuration are created at startup.
 * {@link DataConnection}s added to the configuration dynamically are created via
 * {@link #createConfigDataConnection(DataConnectionConfig)}.
 * {@link DataConnection}s created via SQL are added via {@link #replaceSqlDataConnection(String, String, Map)},
 * these can be removed via {@link #removeDataConnection(String)}.
 * <p>
 * When a new config is added via {@link #createConfigDataConnection(DataConnectionConfig)}
 * and SQL DataConnection with the same name already exists, the SQL DataConnection is
 * removed.
 */
public interface InternalDataConnectionService extends DataConnectionService {

    /**
     * Creates a new DataConnection with the given config.
     * <p>
     * Such DataConnection is considered persistent and can't be removed.
     * The DataConnection is closed when {@link #shutdown()} is called.
     *
     * @param config the configuration of the DataConnection
     */
    void createConfigDataConnection(DataConnectionConfig config);

    /**
     * Creates a new or replaces an existing DataConnection with the given parameters.
     *
     * @param name      name of the DataConnection
     * @param type      type of the DataConnection
     * @param shared    is DataConnection shared
     * @param options options configuring the DataConnection
     */
    void createOrReplaceSqlDataConnection(String name, String type, boolean shared, Map<String, String> options);

    /**
     * Returns if a {@link DataConnection} with given name exists in the config
     *
     * @param name name of the DataConnection
     * @return true if a {@link DataConnection} exists in the config, false otherwise
     */
    boolean existsConfigDataConnection(String name);

    /**
     * Returns if a {@link DataConnection} with given name exists, created by SQL.
     *
     * @param name name of the DataConnection
     * @return true if a {@link DataConnection} exists, created by SQL; false otherwise
     */
    boolean existsSqlDataConnection(String name);


    /**
     * Removes a DataConnection.
     *
     * @param name name of the DataConnection
     * @throws IllegalArgumentException if the DataConnection was created through config,
     *                                  not via {@link #createOrReplaceSqlDataConnection(String, String, boolean, Map)}
     */
    void removeDataConnection(String name);

    /**
     * Return type of the DataConnection with the given name
     * @param name name of the DataConnection
     * @return type of the data connection
     */
    String typeForDataConnection(String name);

    /**
     * Return class implementing DataConnection of given type.
     * @param type type of the DataConnection
     * @return DataConnection implementation class
     */
    Class<? extends DataConnection> classForDataConnectionType(String type);

    /**
     * Close this DataConnectionService, should be called only on member shutdown.
     */
    void shutdown();
}
