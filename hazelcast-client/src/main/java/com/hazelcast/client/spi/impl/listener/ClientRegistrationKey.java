/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;

public class ClientRegistrationKey {

    private final String userRegistrationId;
    private final EventHandler handler;
    private final ListenerMessageCodec codec;

    public ClientRegistrationKey(String userRegistrationId, EventHandler handler, ListenerMessageCodec codec) {
        this.userRegistrationId = userRegistrationId;
        this.handler = handler;
        this.codec = codec;
    }

    public ClientRegistrationKey(String userRegistrationId) {
        this.userRegistrationId = userRegistrationId;
        this.handler = null;
        this.codec = null;
    }

    public ListenerMessageCodec getCodec() {
        return codec;
    }

    public EventHandler getHandler() {
        return handler;
    }

    public String getUserRegistrationId() {
        return userRegistrationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {

            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientRegistrationKey that = (ClientRegistrationKey) o;

        return !(userRegistrationId != null
                ? !userRegistrationId.equals(that.userRegistrationId) : that.userRegistrationId != null);

    }

    @Override
    public int hashCode() {
        return userRegistrationId != null ? userRegistrationId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ClientRegistrationKey{ userRegistrationId='" + userRegistrationId + '\'' + '}';
    }
}
