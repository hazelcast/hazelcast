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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataLink is a reference to a single external system.
 * <p>
 * It contains metadata needed to connect, and might or might not maintain
 * a physical connection or connections, depending on the connector. Typically,
 * it is a {@link java.sql.Connection}, instance of a driver / client to a
 * 3rd party system etc.
 * <p>
 * DataLink is supposed to be used in Jet jobs or SQL queries where the
 * same connection metadata, or even the same connection is to be reused.
 * <p>
 * DataLink is closely related to Jet connectors (sources and sinks) and
 * to {@code SqlConnector}s. While the DataLink handles initialization,
 * maintenance and closing of connections to the external system; the
 * connectors read and write actual data from/into them.
 * <p>
 * Instances of the DataLinks should be obtained by calling
 * {@link DataLinkService#getDataLink} method.
 * <p>
 * Conceptually, there are 3 types of sharing of the underlying
 * instance between multiple (concurrent) Jet jobs or SQL queries:
 * <ul>
 * <li>
 *     Single-use - the instance is created each time it is requested,
 *     it is destroyed after closing. It can be used by single processor
 *     (thread) at a time and after use it cannot be re-used.
 * </li>
 * <li>
 *     Pooled - the instance is obtained from a pool and it is
 *     returned to the pool after closing. It can be used by
 *     a single processor (thread) at a time, but it can be
 *     reused when the processor no longer needs it.
 * </li>
 * <li>
 *     Shared - the instance may be shared by multiple threads,
 *     hence it must be thread-safe, and it is only closed when
 *     the data link is closed.
 * </li>
 * </ul>
 * A job or a SQL query uses the same instance of a DataLink during
 * its whole duration.<br>
 * When a DataLink is removed the existing jobs and SQL queries
 * continue to run without any changes. Subsequent jobs and queries
 * fail.<br>
 * When a DataLink is replaced the existing jobs and queries
 * continue to run without any changes. Subsequent jobs and queries
 * start using the new configuration.
 * <p>
 * To support this behaviour the implementations of this interface
 * must override the {@link #retain()} and {@link #close()} methods
 * to implement reference counting for the shared/pooled instance type.
 * This ensures a correct release of any resources when the last job
 * or query using the data link completes. See the {@link ReferenceCounter}
 * for details how to implement it.
 * <p>
 *
 * @since 5.3
 */
@Beta
public interface DataLink extends AutoCloseable {

    /**
     * Returns the name of this DataLink as specified in the
     * {@link DataLinkConfig} or given to the {@code CREATE DATA LINK}
     * command.
     *
     * @return the name of this DataLink
     */
    String getName();

    /**
     * Returns list of {@link DataLinkResource}s accessible via this DataLink.
     * <p>
     * However, it is not strictly required that the data link lists
     * all resources; a mapping can be created for a resource that is
     * not listed. For example, the list of resources in Oracle might
     * not include tables available through a database link. In fact,
     * it might list no resources at all, perhaps if the security in
     * the target system prevents reading of such a list.
     * <p>
     * The returned list contains up-to-date list of resources.
     * Any changes (added or removed resources) must be reflected in
     * subsequent calls to this method.
     */
    List<DataLinkResource> listResources();

    /**
     * Returns the configuration of this DataLink
     */
    DataLinkConfig getConfig();

    /**
     * Returns the properties / options this DataLink was created with.
     */
    default Map<String, String> options() {
        Map<?, ?> properties = getConfig().getProperties();
        return new HashMap<>((Map<String, String>) properties);
    }

    /**
     * To be used to implement reference counting for the
     * shared / pooled instance of the physical connection.
     * <p>
     * Called by {@link DataLinkService} when the DataLink is retrieved from it.
     * <p>
     * See the {@link ReferenceCounter} for details how to implement it.
     */
    default void retain() {
        // no-op by default
    }

    /**
     * To be used to implement reference counting for the
     * shared / pooled instance of the physical connection.
     * <p>
     * The user must call this method when the DataLink is
     * no longer required.
     * <p>
     * See the {@link ReferenceCounter} for details how to implement it.
     */
    @Override
    default void close() throws Exception {
        // no-op by default
    }
}
