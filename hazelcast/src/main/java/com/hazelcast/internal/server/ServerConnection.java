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

package com.hazelcast.internal.server;

import com.hazelcast.internal.nio.Connection;

/**
 * A ServerConnection is the serverside part of any form of connection on a member. So it could be member to member
 * connection, but it could also be the server side companion of a client to member connection.
 */
public interface ServerConnection extends Connection {

     /**
     * Gets the ServerConnectionManager this ServerConnection belongs to.
     *
     * @return the ServerConnectionManager.
     */
    ServerConnectionManager getConnectionManager();

    /**
     * Returns the connection type.
     *
     * See  {@link com.hazelcast.internal.nio.ConnectionType} for in-house candidates. Note that a type could be
     * provided by a custom client, and it can be a string outside of {@link com.hazelcast.internal.nio.ConnectionType}
     *
     * @return the connection type. It could be that <code>null</code> is returned.
     */
    String getConnectionType();

    /**
     * Sets the type of the connection
     *
     * @param connectionType to be set
     */
    void setConnectionType(String connectionType);

    /**
     * Checks if it is a client connection.
     *
     * @return true if client connection, false otherwise.
     */
    boolean isClient();
}
