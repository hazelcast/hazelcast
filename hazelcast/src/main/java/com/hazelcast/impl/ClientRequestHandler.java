/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ClientService.ClientOperationHandler;
import com.hazelcast.nio.Packet;

import java.util.logging.Logger;

public class ClientRequestHandler implements Runnable {
    private final Packet packet;
    private final CallContext callContext;
    private final Node node;
    Logger logger = Logger.getLogger(this.getClass().getName());
    
    private final ClientOperationHandler[] clientOperationHandlers;    

    public ClientRequestHandler(Node node, Packet packet, CallContext callContext, ClientOperationHandler[] clientOperationHandlers) {
        this.packet = packet;
        this.callContext = callContext;
        this.node = node;
        this.clientOperationHandlers = clientOperationHandlers;
    }

    public void run() {
        ThreadContext.get().setCallContext(callContext);
        ClientOperationHandler clientOperationHandler = clientOperationHandlers[packet.operation.getValue()];
        if(clientOperationHandler!=null){
        	clientOperationHandler.handle(node, packet);
        	return;
        }
        else{
        	throw new RuntimeException("Unknown Client Operation, can not handle " + packet.operation);
        }
    }        
}
