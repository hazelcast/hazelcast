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

package com.hazelcast.dataconnection;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;

import javax.annotation.Nonnull;

/**
 * The interface provides access to {@link DataConnection}s for Jet processors. A Jet
 * processor can obtain a reference to the service via {@link
 * ProcessorMetaSupplier.Context#dataConnectionService()}.
 * <p>
 * Data connections can be created via one of the following ways:
 * <ul>
 *     <li>statically in the configuration
 *     <li>dynamically via {@link Config#addDataConnectionConfig(DataConnectionConfig)}
 *     <li>via SQL {@code CREATE DATA CONNECTION ...} command
 * </ul>
 *
 * @since 5.3
 */
public interface DataConnectionService {

    /**
     * Returns {@link DataConnection} with the given name and `retain` it (calls
     * {@link DataConnection#retain()}). The caller is responsible for calling
     * {@link DataConnection#release()} after it is done with the DataConnection.
     * <p>
     * Type is checked against the provided `clazz` argument.
     *
     * @param name  name of the DataConnection
     * @param clazz expected type of the DataConnection
     *
     * @throws HazelcastException if the requested DataConnection doesn't exist, or has
     *     a different type than `clazz`
     */
    @Nonnull
    <T extends DataConnection> T getAndRetainDataConnection(String name, Class<T> clazz);

}
