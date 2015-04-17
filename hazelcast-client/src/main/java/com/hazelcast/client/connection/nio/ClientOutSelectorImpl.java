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

package com.hazelcast.client.connection.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.tcp.AbstractIOSelector;
import com.hazelcast.nio.tcp.IOSelectorOutOfMemoryHandler;
import com.hazelcast.nio.tcp.SelectionHandler;

import java.nio.channels.SelectionKey;


public final class ClientOutSelectorImpl extends AbstractIOSelector {

    public ClientOutSelectorImpl(ThreadGroup threadGroup, String tname, ILogger logger,
                                 IOSelectorOutOfMemoryHandler oomeHandler) {
        super(threadGroup, tname, logger, oomeHandler);
    }

    @Override
    protected void handleSelectionKey(SelectionKey sk) {
        if (sk.isValid() && sk.isWritable()) {
            sk.interestOps(sk.interestOps() & ~SelectionKey.OP_WRITE);
            SelectionHandler handler = (SelectionHandler) sk.attachment();
            handler.handle();
        }
    }
}

