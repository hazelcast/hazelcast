/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.instance.CallContext;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;

public abstract class ClientRequestHandler{
    protected final CallContext callContext;
    protected final Node node;
    protected final ILogger logger;
    protected final Subject subject;
    protected final Connection connection;
    private volatile Thread runningThread = null;
    private volatile boolean valid = true;

    public ClientRequestHandler(CallContext callContext, Subject subject, Node node, Connection connection) {
        this.callContext = callContext;
        this.subject = subject;
        this.logger = node.getLogger(ClientRequestHandler.class.getName());
        this.node = node;
        this.connection = connection;
    }

    public void cancel() {
        valid = false;
        if (runningThread != null) {
            runningThread.interrupt();
        }
    }


    public void doRun() {
        runningThread = Thread.currentThread();
        ThreadContext.get().setCallContext(callContext);
        try {
            if (!valid) return;
            final PrivilegedExceptionAction<Void> action = createAction();
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

    protected abstract PrivilegedExceptionAction<Void> createAction();
}
