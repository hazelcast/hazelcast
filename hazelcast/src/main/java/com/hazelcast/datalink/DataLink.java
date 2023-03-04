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
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * DataLink is a reference to a single external system.
 * <p>
 * It contains metadata needed to connect, and might or might not maintain
 * a physical connection or connections, depending on the connector. The connection
 * might be an instance of {@link java.sql.Connection}, or of a driver or client to a
 * 3rd party system etc.
 * <p>
 * Every connection obtained from a DataLink must be closed. Otherwise, the data
 * link cannot be closed and the connection will leak.
 * <p>
 * DataLink is supposed to be used in Jet jobs or SQL mappings where the
 * same connection metadata, or even the same connection is to be reused.
 * <p>
 * DataLink is closely related to Jet connectors (sources and sinks) and
 * to {@code SqlConnector}s. While the DataLink handles initialization,
 * maintenance and closing of connections to the external system, the
 * connections read and write actual data from/into them.
 * <p>
 * Instances of DataLinks should be obtained by calling
 * {@link DataLinkService#getAndRetainDataLink} method.
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
 *     the data link is closed.
 * </li>
 * </ul>
 * However, the connector might implement any other appropriate strategy.
 * <p>
 * When a DataLink is closed, connections obtained from should continue to be
 * functional, until all connections are returned. Replacing of a DataLink is
 * handled as remove+create.
 * <p>
 * Implementations of DataLink must provide a constructor with a single argument
 * of type {@link DataLinkConfig}.
 *
 * @since 5.3
 */
@Beta
public interface DataLink {

    /**
     * Returns the name of this DataLink as specified in the
     * {@link DataLinkConfig} or the {@code CREATE DATA LINK}
     * command.
     *
     * @return the name of this DataLink
     */
    @Nonnull
    String getName();

    /**
     * Returns list of {@link DataLinkResource}s accessible via this DataLink.
     * <p>
     * It is not strictly required that the data link lists all resources; a
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
    Collection<DataLinkResource> listResources();

    /**
     * Returns the configuration of this DataLink.
     */
    @Nonnull
    DataLinkConfig getConfig();

    /**
     * Returns the properties / options this DataLink was created with.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    default Map<String, String> options() {
        Map<?, ?> properties = getConfig().getProperties();
        return new HashMap<>((Map<String, String>) properties);
    }

    /**
     * Prevents the data link from being closed. It is useful when the processor
     * wants to avoid the data link from being closed while it is obtaining
     * connections from it. Multiple threads can retain the same DataLink
     * concurrently.
     * <p>
     * Note that the DataLink also isn't closed until all shared connections
     * obtained from it are returned. This feature, together with the lock
     * allows the processor to avoid concurrent close while it is using the
     * connection.
     *
     * @throws IllegalStateException fi the data link is already closed
     */
    void retain();

    /**
     * Release a retained data link. Must be called after every {@link
     * #retain()} call, otherwise the data link will leak.
     */
    void release();

    /**
     * Called by the member when shutting down. Should unconditionally close all
     * connections and release resources.
     */
    void destroy();
}
