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

package com.hazelcast.client;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

/**
 * ClientService allows you to query connected {@link Client}s and
 * attach/detach {@link ClientListener}s to listen to connection events.
 * <p>
 * All the methods are thread-safe.
 *
 * @see Client
 * @see ClientListener
 */
public interface ClientService {

    /**
     * Returns all connected clients to this member.
     *
     * @return all connected clients to this member
     */
    @Nonnull
    Collection<Client> getConnectedClients();

    /**
     * Adds a ClientListener.
     *
     * When a ClientListener is added more than once, it will receive duplicate events.
     *
     * @param clientListener the ClientListener to add
     * @return registration ID which can be used to remove the listener using the {@link #removeClientListener(UUID)} method
     * @throws java.lang.NullPointerException if clientListener is {@code null}
     */
    @Nonnull UUID addClientListener(@Nonnull ClientListener clientListener);

    /**
     * Removes a ClientListener.
     *
     * Can safely be called with a non existing ID, or when the ClientListener already is removed.
     *
     * @param registrationId ID of the ClientListener registration
     * @return {@code true} if registration is removed, {@code false} otherwise
     * @throws java.lang.NullPointerException if registration ID is {@code null}
     */
    boolean removeClientListener(@Nonnull UUID registrationId);
}
