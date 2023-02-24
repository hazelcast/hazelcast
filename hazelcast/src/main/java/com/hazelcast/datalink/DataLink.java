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
 * same connection metadata, or even the same connection is to be reused,
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
     * Returns list of {@link Resource}s accessible via this DataLink.
     * <p>
     * However, it is not strictly required that the data link lists
     * all resources; a mapping can be created for a resource that is
     * not listed. For example, the list of resources in Oracle might
     * not include tables available through a database link. In fact,
     * it might list no resources at all, perhaps if the security in
     * the target system prevents reading of such a list.
     */
    List<Resource> listResources();

    /**
     * Returns the configuration of this DataLink
     */
    DataLinkConfig getConfig();

    void retain();

    /**
     * Returns the properties / options this DataLink was created with.
     */
    default Map<String, String> options() {
        Map<?, ?> properties = getConfig().getProperties();
        return new HashMap<>((Map<String, String>) properties);
    }

    /**
     * Closes underlying resources
     */
    @Override
    default void close() throws Exception {
        //no op
    }
}
