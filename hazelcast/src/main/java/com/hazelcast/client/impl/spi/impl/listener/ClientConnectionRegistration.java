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

package com.hazelcast.client.impl.spi.impl.listener;

import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Keeps the information related to to an event registration made by clients.
 */
public class ClientConnectionRegistration {

    private final UUID serverRegistrationId;
    private final long callId;

    ClientConnectionRegistration(UUID serverRegistrationId, long callId) {
        isNotNull(serverRegistrationId, "serverRegistrationId");
        this.serverRegistrationId = serverRegistrationId;
        this.callId = callId;
    }

    /**
     * Alias registration ID is same as registration ID in the beginning. If listener had to be re-registered
     * new registration ID is stored as server registration ID.
     * When user try to remove the listener with registration ID, related server registration is send to
     * subscribed member to remove the listener.
     *
     * @return server registration ID
     */
    public UUID getServerRegistrationId() {
        return serverRegistrationId;
    }


    /**
     * Call ID of first event registration request
     *
     * @return call ID
     */
    long getCallId() {
        return callId;
    }

}
