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

package com.hazelcast.client.spi.impl.listener;

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.nio.Address;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Keeps the information related to to an event registration made by clients
 */
public class ClientEventRegistration {

    private Address subscriber;
    private final String serverRegistrationId;
    private final int callId;
    private BaseClientRemoveListenerRequest removeRequest;

    public ClientEventRegistration(String serverRegistrationId,
                                   int callId, Address subscriber, BaseClientRemoveListenerRequest removeRequest) {
        this.removeRequest = removeRequest;
        isNotNull(serverRegistrationId, "serverRegistrationId");
        this.serverRegistrationId = serverRegistrationId;
        this.callId = callId;
        this.subscriber = subscriber;
    }


    /**
     * Alias registration id is same as registration id in the beginning. If listener had to be re-registered
     * new registration id is stored as server registration id.
     * When user try to remove the listener with registration id, related server registration is send to
     * subscribed member to remove the listener.
     *
     * @return server registration Id
     */
    public String getServerRegistrationId() {
        return serverRegistrationId;
    }


    /**
     * This is used when removing the listener.
     * Note: Listeners need to be removed from the member that they are first subscribed.
     *
     * @return subscriber
     */
    public Address getSubscriber() {
        return subscriber;
    }

    /**
     *
     * @return request that will remove the listener from the remote
     */
    public BaseClientRemoveListenerRequest getRemoveRequest() {
        return removeRequest;
    }

    /**
     * Call id of first event registration request
     *
     * @return call id
     */
    public int getCallId() {
        return callId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientEventRegistration that = (ClientEventRegistration) o;

        return serverRegistrationId.equals(that.serverRegistrationId);

    }

    @Override
    public int hashCode() {
        return serverRegistrationId.hashCode();
    }
}

