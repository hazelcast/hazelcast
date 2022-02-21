/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * A single entry point for all interactions with other member services.
 */
public interface NodeServiceProvider extends ClockProvider, LocalMemberIdProvider {
    /**
     * Get IDs of data members known to the member.
     *
     * @return IDs of data members.
     */
    Collection<UUID> getDataMemberIds();

    /**
     * @return IDs of active clients.
     */
    Set<UUID> getClientIds();

    /**
     * Get connection to member.
     *
     * @param memberId Member ID.
     * @return Connection object or {@code null} if the connection cannot be established.
     */
    Connection getConnection(UUID memberId);

    /**
     * @param name Map name.
     * @return Container or {@code null} if map with the given name doesn't exist.
     */
    MapContainer getMap(String name);

    /**
     * Get logger for class.
     *
     * @param clazz Class.
     * @return Logger.
     */
    ILogger getLogger(Class<?> clazz);
}
