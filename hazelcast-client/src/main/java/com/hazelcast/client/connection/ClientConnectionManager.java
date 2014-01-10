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
import com.hazelcast.nio.Address;

import java.io.IOException;

/**
 * @author ali 5/27/13
 */
public interface ClientConnectionManager {

    public void shutdown();

    public void start();

    public ClientConnection getRandomConnection() throws IOException;

    public ClientConnection getOrConnect(Address address) throws IOException ;

    public ClientConnection ownerConnection(Address address, Authenticator authenticator) throws IOException ;
}
