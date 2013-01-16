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

package com.hazelcast.impl;

import com.hazelcast.impl.ClientHandlerService.ClientOperationHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;

public class ClientRequestHandler extends FallThroughRunnable {
    private final Packet packet;
    private final CallContext callContext;
    private final Node node;
    private final ClientHandlerService.ClientOperationHandler clientOperationHandler;
    private volatile Thread runningThread = null;
    private volatile boolean valid = true;
    private final ILogger logger;
    private final Subject subject;

    public ClientRequestHandler(Node node, Packet packet, CallContext callContext,
                                ClientOperationHandler clientOperationHandler, Subject subject) {
        this.packet = packet;
        this.callContext = callContext;
        this.node = node;
        this.clientOperationHandler = clientOperationHandler;
        this.logger = node.getLogger(this.getClass().getName());
        this.subject = subject;
    }

    @Override
    public void doRun() {
        runningThread = Thread.currentThread();
        ThreadContext.get().setCallContext(callContext);
        try {
            if (!valid) return;
            final PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
                public Void run() {
                    Connection connection = packet.conn;
                    clientOperationHandler.handle(node, packet);
                    node.clientHandlerService.getClientEndpoint(connection).removeRequest(ClientRequestHandler.this);
                    return null;
                }
            };
            if (node.securityContext == null) {
                action.run();
            } else {
                node.securityContext.doAsPrivileged(subject, action);
            }
        } catch (Throwable e) {
            logger.log(Level.WARNING, e.getMessage(), e);
            if (node.isActive()) {
                throw (RuntimeException) e;
            }
        } finally {
            runningThread = null;
        }
    }

    public void cancel() {
        valid = false;
        if (runningThread != null) {
            runningThread.interrupt();
        }
    }
}
