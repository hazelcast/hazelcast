/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.AuthenticationException;

import java.io.IOException;

/**
 * The Authenticator is responsible for authenticating a {@link ClientConnection}.
 */
public interface Authenticator {

    /**
     * Authenticates a ClientConnection. If the call returns normally, the ClientConnection is authenticated
     * successfully.
     *
     * @param connection the ClientConnection
     * @throws AuthenticationException if the authentication failed
     * @throws IOException if some kind of IO problem happeend.
     */
    void auth(ClientConnection connection) throws AuthenticationException, IOException;
}
