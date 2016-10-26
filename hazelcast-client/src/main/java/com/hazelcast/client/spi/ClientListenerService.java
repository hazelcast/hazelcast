/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;

/**
 * Client service to add/remove remote listeners.
 *
 * For smart client, it registers local  listeners to all nodes in cluster.
 * For dummy client, it registers global listener to one node.
 */
public interface ClientListenerService {

    String registerListener(ListenerMessageCodec listenerMessageCodec, EventHandler handler);

    boolean deregisterListener(String registrationId);

    /**
     * Do the appropriate action based on connection to the cluster
     * @param clientConnection The connection that the client is using for cluster connection.
     */
    void onClusterConnect(ClientConnection clientConnection);
}
