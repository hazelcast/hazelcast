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
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.util.ExceptionUtil;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;

public final class ClientRequestHandler implements Runnable {
    private final Node node;
    private final Protocol protocol;
    private final ClientEndpoint clientEndpoint;
    private final ILogger logger;

    private volatile Thread runningThread = null;
    private volatile boolean valid = true;

    public ClientRequestHandler(Node node, ClientEndpoint clientEndpoint, Protocol protocol) {
        this.clientEndpoint = clientEndpoint;
        this.protocol = protocol;
        this.node = node;
        this.logger = node.getLogger(ClientRequestHandler.class.getName());
    }

    private void run0() {
        runningThread = Thread.currentThread();
        final ThreadContext threadContext = ThreadContext.getOrCreate();
        threadContext.setCallerUuid(clientEndpoint.uuid);
        try {
            if (!valid) return;
            if (node.securityContext == null) {
                handleCommand();
            } else {
                final Subject subject = clientEndpoint.getSubject();
                final PrivilegedExceptionAction<Void> action = new ClientPrivilegedAction();
                node.securityContext.doAsPrivileged(subject, action);
            }
        } catch (Throwable e) {
            logger.log(Level.WARNING, e.getMessage(), e);
            if (node.isActive()) {
                throw ExceptionUtil.rethrow(e);
            }
        } finally {
            runningThread = null;
            threadContext.setCallerUuid(null);
        }
    }

    private void handleCommand() {
        TcpIpConnection connection = protocol.conn;
        ClientCommandHandler commandHandler = node.clientCommandService.getService(protocol);
        commandHandler.handle(node, protocol);
        node.clientCommandService.getClientEndpoint(connection).removeRequest(this);
    }

    private class ClientPrivilegedAction implements PrivilegedExceptionAction<Void> {
        public Void run() throws Exception {
            handleCommand();
            return null;
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
