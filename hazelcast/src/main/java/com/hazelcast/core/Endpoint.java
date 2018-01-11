/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.net.SocketAddress;

/**
 * Endpoint represents a peer in the cluster.
 * It can be either a member or a client.
 *
 * @see Member
 * @see Client
 */
public interface Endpoint {

    /**
     * Returns the unique UUID for this endpoint.
     *
     * @return the unique UUID for this endpoint
     */
    String getUuid();

    /**
     * Returns the socket address for this endpoint.
     *
     * @return the socket address for this endpoint
     */
    SocketAddress getSocketAddress();
}
