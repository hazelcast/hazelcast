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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.client.ClientEndpoint;

public final class CacheInvalidationListener implements CacheEventListener {

    private final ClientEndpoint endpoint;
    private final int callId;

    public CacheInvalidationListener(ClientEndpoint endpoint, int callId) {
        this.endpoint = endpoint;
        this.callId = callId;
    }

    @Override
    public void handleEvent(Object eventObject) {
        if (eventObject instanceof CacheInvalidationMessage) {
            CacheInvalidationMessage message = (CacheInvalidationMessage) eventObject;
            if (endpoint.isAlive()) {
                endpoint.sendEvent(message.getKey(), message, callId);
            }
        }
    }

}
