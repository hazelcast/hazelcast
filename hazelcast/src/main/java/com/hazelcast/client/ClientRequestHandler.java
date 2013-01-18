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

package com.hazelcast.client;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.util.Util;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;

public class ClientRequestHandler implements Runnable {
    private final Protocol protocol;
//    private final CallContext callContext;
    private final Node node;
    //    private final ClientHandlerService.ClientCommandHandler clientOperationHandler;
    private volatile Thread runningThread = null;
    private volatile boolean valid = true;
    private final ILogger logger;
    private final Subject subject;

    public ClientRequestHandler(Node node, Protocol protocol, Subject subject) {
        this.protocol = protocol;
//        this.callContext = callContext;
        this.node = node;
        this.logger = node.getLogger(ClientRequestHandler.class.getName());
        this.subject = subject;
    }

    public void run0() {
        runningThread = Thread.currentThread();
//        ThreadContext.get().setCallContext(callContext);
        try {
            if (!valid) return;
            final PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
                public Void run() {
                    TcpIpConnection connection = protocol.conn;
                    ClientCommandHandler clientOperationHandler = node.clientCommandService.getService(protocol);
                    System.out.println(protocol.command + "clientOperationHandler = " + clientOperationHandler);
                        clientOperationHandler.handle(node, protocol);
                    node.clientCommandService.getClientEndpoint(connection).removeRequest(ClientRequestHandler.this);
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
                Util.throwUncheckedException(e);
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

    public void run() {
        try {
            run0();
        } catch (Throwable ignored) {
        }
    }
}
