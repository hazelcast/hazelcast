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

import java.util.EventListener;

/**
 * ClientListener allows to get notified when a {@link Client} is connected to
 * or disconnected from cluster.
 *
 * @see Client
 * @see ClientService#addClientListener(ClientListener)
 */
public interface ClientListener extends EventListener {

    /**
     * Invoked when a new client is connected.
     *
     * @param client Client instance
     */
    void clientConnected(Client client);

    /**
     * Invoked when a new client is disconnected.
     *
     * @param client Client instance
     */
    void clientDisconnected(Client client);
}
