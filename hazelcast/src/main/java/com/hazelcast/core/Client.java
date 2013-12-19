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

package com.hazelcast.core;

import java.net.InetSocketAddress;

/**
 * Client interface allows to get information about
 * a connected clients socket address, type and uuid.
 *
 * @see ClientService
 * @see ClientListener
 */
public interface Client extends Endpoint {

    /**
     * Returns unique uuid for this client
     *
     * @return unique uuid for this client
     */
    String getUuid();

    /**
     * Returns socket address of this client
     *
     * @return socket address of this client
     */
    InetSocketAddress getSocketAddress();

    /**
     * Returns type of this client
     *
     * @return type of this client
     */
    ClientType getClientType();
}
