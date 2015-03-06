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

package com.hazelcast.client.impl.messagehandlers;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageHandler;
import com.hazelcast.client.impl.protocol.MessageHandlerContext;
import com.hazelcast.client.impl.protocol.MessageHandlerParameters;

/**
 * Base support class for message handlers
 */
public abstract class AbstractMessageHandler implements MessageHandler {

    @Override
    public MessageHandlerParameters createParameters() {
        return null;
    }

    protected static ClientMessage prepareResponse(MessageHandlerContext context, Object... objects) {

        return null;
    }

    protected static ClientMessage prepareException(MessageHandlerContext context, Throwable exception) {

        return null;
    }
}
