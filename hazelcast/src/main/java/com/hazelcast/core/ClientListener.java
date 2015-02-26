/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.EventListener;

/**
 * The ClientListener provides the ability to listen to clients connecting and disconnecting from the cluster.
 *
 * @see Client
 * @see ClientService#addClientListener(ClientListener)
 */
public interface ClientListener extends EventListener {

    /**
     * Invoked when a new client is connected.
     *
     * @param client the client instance
     */
    void clientConnected(Client client);

    /**
     * Invoked when a new client is disconnected.
     *
     * @param client the client instance
     */
    void clientDisconnected(Client client);
}
