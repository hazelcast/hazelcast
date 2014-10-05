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

import java.util.Collection;

/**
 * ClientService allows to query connected {@link Client}s and
 * attach/detach {@link ClientListener}s to listen connection events.
 *
 * All the methods are thread-safe.
 *
 * @see Client
 * @see ClientListener
 */
public interface ClientService {

    /**
     * Returns all connected clients to this member.
     *
     * @return all connected clients to this member.
     */
    Collection<Client> getConnectedClients();

    /**
     * Adds a ClientListener
     *
     * When a ClientListener is added more than once, it will receive duplicate events.
     *
     * @param clientListener ClientListener
     * @return returns registration id.
     * @throws java.lang.NullPointerException if clientListener is null.
     */
    String addClientListener(ClientListener clientListener);

    /**
     * Removes a ClientListener.
     *
     * Can safely be called with a non existing id, or when the ClientListener already is removed.
     *
     * @param registrationId Id of listener registration.
     * @return true if registration is removed, false otherwise
     * @throws java.lang.NullPointerException if registrationId is null.
     */
    boolean removeClientListener(String registrationId);
}
