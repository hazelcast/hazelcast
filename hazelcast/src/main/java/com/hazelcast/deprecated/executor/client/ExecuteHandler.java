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

package com.hazelcast.deprecated.executor.client;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorServiceProxy;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;

import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class ExecuteHandler extends ExecutorCommandHandler {
    final DistributedExecutorService distributedExecutorService;

    public ExecuteHandler(DistributedExecutorService distributedExecutorService) {
        super(distributedExecutorService);
        this.distributedExecutorService = distributedExecutorService;
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        return null;
    }

    @Override
    public void handle(Node node, Protocol protocol) {
        Protocol response = null;
        try {
            String name = protocol.args[0];
            Callable<Object> callable = (Callable<Object>) node.serializationService.toObject(protocol.buffers[0]);
            ExecutorServiceProxy proxy = getExecutorServiceProxy(name);
            MyExecutionCallBack callBack = new MyExecutionCallBack(protocol, node);
            if (protocol.args.length > 1) {
                try {
                    String hostName = protocol.args[1];
                    int port = Integer.valueOf(protocol.args[2]);
                    Address address = new Address(hostName, port);
                    boolean local = node.getLocalMember().getAddress().equals(address);
                    MemberImpl member = new MemberImpl(address, local);
                    proxy.submitToMember(callable, member, callBack);
                } catch (UnknownHostException e) {
                    response = protocol.error(new Data[]{node.serializationService.toData(e)}, e.getClass().getName());
                }
            } else {
                Data key = protocol.buffers[1];
                proxy.submitToKeyOwner(callable, node.serializationService.toObject(key), callBack);
            }
        } catch (HazelcastInstanceNotActiveException e) {
            Data exception  = node.serializationService.toData(e);
            response = new Protocol(protocol.conn, Command.ERROR, protocol.flag, protocol.threadId, false, new String[]{e.getClass().getName()}, exception);
        } catch (RuntimeException e) {
            ILogger logger = node.getLogger(this.getClass().getName());
            logger.log(Level.WARNING,
                    "exception during handling " + protocol.command + ": " + e.getMessage(), e);
            Data exception  = node.serializationService.toData(e);
            response = new Protocol(protocol.conn, Command.ERROR, protocol.flag, protocol.threadId, false, new String[]{e.getClass().getName()}, exception);
        }
        if (response != null)
            sendResponse(node, response, protocol.conn);
    }

    class MyExecutionCallBack implements ExecutionCallback<Object> {

        private final Protocol protocol;
        private final Node node;

        MyExecutionCallBack(Protocol protocol, Node node) {
            this.protocol = protocol;
            this.node = node;
        }

        @Override
        public void onResponse(Object response) {
            sendResponse(node, protocol.success(node.serializationService.toData(response)), protocol.conn);
        }

        @Override
        public void onFailure(Throwable t) {
            sendResponse(node, protocol.error(new Data[]{node.serializationService.toData(t)}, t.getClass().getName()), protocol.conn);
        }
    }
}
