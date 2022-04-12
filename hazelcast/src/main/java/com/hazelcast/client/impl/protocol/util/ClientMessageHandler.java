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
package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;

import java.util.function.Consumer;

public final class ClientMessageHandler {

    private ClientMessageHandler() {
    }

    public static void accept(ClientListenerServiceImpl listenerService, Consumer<ClientMessage> responseHandler,
                              ClientMessage message) {
        if (ClientMessage.isFlagSet(message.getHeaderFlags(), ClientMessage.BACKUP_EVENT_FLAG)) {
            responseHandler.accept(message);
        } else if (ClientMessage.isFlagSet(message.getHeaderFlags(), ClientMessage.IS_EVENT_FLAG)) {
            listenerService.handleEventMessage(message);
        } else {
            responseHandler.accept(message);
        }
    }
}
