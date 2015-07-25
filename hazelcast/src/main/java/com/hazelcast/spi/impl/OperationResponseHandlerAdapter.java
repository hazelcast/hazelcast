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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;

/**
 * An abstract OperationResponseHandler adapter. It doesn't adapt one interface to another; the traditional adapter design
 * pattern. The same technique is applied in e.g. {@link java.awt.event.MouseAdapter} that implements
 * {@link java.awt.event.MouseListener}.
 *
 * It implements all methods so that only methods that are interesting need to be implemented.
 *
 * Every implemented method forwards to the {@link #onSend()} method. So by overriding this method, any send method can be
 * intercepted.
 */
public abstract class OperationResponseHandlerAdapter implements OperationResponseHandler {

    @Override
    public void sendNormalResponse(Operation op, Object response, int syncBackupCount) {
        onSend();
    }

    @Override
    public void sendBackupComplete(Address address, long callId, boolean urgent) {
        onSend();
    }

    @Override
    public void sendErrorResponse(Address address, long callId, boolean urgent, Operation op, Throwable cause) {
        onSend();
    }

    @Override
    public void sendTimeoutResponse(Operation op) {
        onSend();
    }

    public void onSend() {
    }

    @Override
    public boolean isLocal() {
        return true;
    }
}
