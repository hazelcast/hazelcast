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

import com.hazelcast.config.DataConnectionConfig;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * DataConnection is a reference to a single external system.
 * <p>
 * It contains metadata needed to connect, and might or might not maintain
 * a physical connection or connections, depending on the connector. The connection
 * might be an instance of {@link java.sql.Connection}, or of a driver or client to a
 * 3rd party system etc.
 * <p>
 * Every connection obtained from a DataConnection must be closed. Otherwise, the data
 * link cannot be closed and the connection will leak.
 * <p>
 * DataConnection is supposed to be used in Jet jobs or SQL mappings where the
 * same connection metadata, or even the same connection is to be reused.
 * <p>
 * DataConnection is closely related to Jet connectors (sources and sinks) and
 * to {@code SqlConnector}s. While the DataConnection handles initialization,
 * maintenance and closing of connections to the external system, the
 * connections read and write actual data from/into them.
 * <p>
 * Instances of DataConnections should be obtained by calling
 * {@link DataConnectionService#getAndRetainDataConnection} method.
 * <p>
 * Conceptually, there are 3 types of sharing of the underlying
 * instance between multiple (concurrent) Jet jobs or SQL queries:
 * <ul>
 * <li>
 *     Single-use - the instance is created each time it is requested,
 *     it is destroyed after closing. It can be used by a single processor
 *     (thread) at a time and after use it cannot be re-used.
 * </li>
 * <li>
 *     Pooled - the instance is obtained from a pool and it is
 *     returned to the pool after closing. It can be used by
 *     a single processor (thread) at a time, but it is reused
 *     when the processor no longer needs it.
 * </li>
 * <li>
 *     Shared - the instance may be shared by multiple threads,
 *     hence it must be thread-safe, and it is only closed when
 *     the data connection is closed.
 * </li>
 * </ul>
 * However, the connector might implement any other appropriate strategy.
 * <p>
 * When a DataConnection is closed, connections obtained from should continue to be
 * functional, until all connections are returned. Replacing of a DataConnection is
 * handled as remove+create.
 * <p>
 * Implementations of DataConnection must provide a constructor with a single argument
 * of type {@link DataConnectionConfig}. The constructor must not throw an exception
 * under any circumstances. Any resource allocation should be done either asynchronously
 * or lazily when the underlying instance is requested.
 *
 * @since 5.3
 */
public interface DataConnection {

    /**
     * Returns the name of this data connection as specified in the
     * {@link DataConnectionConfig} or the {@code CREATE DATA CONNECTION}
     * command.
     *
     * @return the name of this DataConnection
     */
    @Nonnull
    String getName();

    /**
     * Returns list of {@link DataConnectionResource}s accessible via this DataConnection.
     * <p>
     * It is not strictly required that the data connection lists all resources; a
     * resource can be used even if it is not listed. For example, the list of
     * resources in Oracle database might not include tables available through a
     * database link. In fact, it might list no resources at all, perhaps if the
     * security in the target system prevents reading of such a list.
     * <p>
     * The returned list contains up-to-date list of resources.
     * Any changes (added or removed resources) must be reflected in
     * subsequent calls to this method.
     */
    @Nonnull
    Collection<DataConnectionResource> listResources();

    /**
     * Returns the list of possible values for {@link DataConnectionResource#type()},
     * that will be returned when {@link #listResources()} is called. Returned values are case-insensitive,
     * e.g. {@link DataConnectionResource#type()} may return {@code MY_RES} and this method {@code my_res}.
     */
    @Nonnull
    Collection<String> resourceTypes();

    /**
     * Returns the configuration of this DataConnection.
     */
    @Nonnull
    DataConnectionConfig getConfig();

    /**
     * Returns the properties / options this DataConnection was created with.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    default Map<String, String> options() {
        Map<?, ?> properties = getConfig().getProperties();
        return new HashMap<>((Map<String, String>) properties);
    }

    /**
     * Prevents the data connection from being closed. It is useful when the processor
     * wants to avoid the data connection from being closed while it is obtaining
     * connections from it. Multiple threads can retain the same DataConnection
     * concurrently.
     * <p>
     * Note that the DataConnection also isn't closed until all shared connections
     * obtained from it are returned. This feature, together with the lock
     * allows the processor to avoid concurrent close while it is using the
     * connection.
     *
     * @throws IllegalStateException fi the data connection is already closed
     */
    void retain();

    /**
     * Release a retained data connection. Must be called after every {@link
     * #retain()} call, otherwise the data connection will leak.
     */
    void release();

    /**
     * Called by the member when shutting down. Should unconditionally close all
     * connections and release resources.
     */
    void destroy();
}
