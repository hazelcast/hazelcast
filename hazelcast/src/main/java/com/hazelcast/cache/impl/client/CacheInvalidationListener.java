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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.client.ClientEndpoint;

import java.util.ArrayList;
import java.util.List;

public final class CacheInvalidationListener implements CacheEventListener {

    private final ClientEndpoint endpoint;
    private final int callId;

    public CacheInvalidationListener(ClientEndpoint endpoint, int callId) {
        this.endpoint = endpoint;
        this.callId = callId;
    }

    @Override
    public void handleEvent(Object eventObject) {
        if (!endpoint.isAlive()) {
            return;
        }
        if (eventObject instanceof CacheInvalidationMessage) {
            String targetUuid = endpoint.getUuid();
            if (eventObject instanceof CacheSingleInvalidationMessage) {
                CacheSingleInvalidationMessage message = (CacheSingleInvalidationMessage) eventObject;
                if (!targetUuid.equals(message.getSourceUuid())) {
                    // Since we already filtered as source uuid, no need to send source uuid to client
                    // We don't set it null at here because the same message instance is also used by
                    // other listeners in this node. So we create a new, fresh and clean message instance.
                    // TODO Maybe don't send name also to client
                    endpoint.sendEvent(message.getName(),
                                       new CacheSingleInvalidationMessage(message.getName(), message.getKey(), null),
                                       callId);
                }
            } else if (eventObject instanceof CacheBatchInvalidationMessage) {
                CacheBatchInvalidationMessage message = (CacheBatchInvalidationMessage) eventObject;
                List<CacheSingleInvalidationMessage> invalidationMessages =
                        message.getInvalidationMessages();
                List<CacheSingleInvalidationMessage> filteredMessages =
                        new ArrayList<CacheSingleInvalidationMessage>(invalidationMessages.size());
                for (CacheSingleInvalidationMessage invalidationMessage : invalidationMessages) {
                    if (!targetUuid.equals(invalidationMessage.getSourceUuid())) {
                        // Since we already filtered as source uuid, no need to send source uuid to client
                        // Also no need to send name to client because single invalidation messages
                        // are already wrapped by name batch invalidation message. We don't set them null at here
                        // because the same message instance is also used by other listeners in this node.
                        // So we create a new, fresh and clean message instance.
                        filteredMessages.add(
                                new CacheSingleInvalidationMessage(null, invalidationMessage.getKey(), null));
                    }
                }
                // TODO Maybe don't send name also to client
                endpoint.sendEvent(message.getName(),
                                   new CacheBatchInvalidationMessage(message.getName(), filteredMessages),
                                   callId);
            }
        }
    }

}
