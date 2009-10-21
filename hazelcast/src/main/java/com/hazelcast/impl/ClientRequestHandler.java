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

import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import static com.hazelcast.core.Transaction.TXN_STATUS_ACTIVE;
import com.hazelcast.impl.BaseManager.KeyValue;
import com.hazelcast.impl.ClientService.ClientEndpoint;
import com.hazelcast.impl.ConcurrentMapManager.Entries;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import com.hazelcast.nio.Data;
import static com.hazelcast.nio.IOUtil.*;
import com.hazelcast.nio.Packet;

import java.util.List;
import java.util.logging.Logger;

public class ClientRequestHandler implements Runnable {
    private final Packet packet;
    private final CallContext callContext;
    private final Node node;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public ClientRequestHandler(Node node, Packet packet, CallContext callContext) {
        this.packet = packet;
        this.callContext = callContext;
        this.node = node;
    }

    public void run() {
        ThreadContext.get().setCallContext(callContext);
        if (packet.operation.equals(ClusterOperation.CONCURRENT_MAP_PUT)) {
//        	logger.info("PUT" + System.currentTimeMillis());
        	IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            Object oldValue = map.put(packet.key, packet.value);
//            System.out.println("Value:  " + toObject(doHardCopy(packet.value)));
            packet.value = (Data) oldValue;
            
            sendResponse(packet);
        } else if (packet.operation.equals(ClusterOperation.CONCURRENT_MAP_GET)) {
            IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            Object value = map.get(packet.key);
            Data data = (Data) value;
            if (callContext.getCurrentTxn() != null && callContext.getCurrentTxn().getStatus() == TXN_STATUS_ACTIVE) {
                data = data;
            }
            packet.value = data;
            sendResponse(packet);
        } else if (packet.operation.equals(ClusterOperation.TRANSACTION_BEGIN)) {
            Transaction transaction = node.factory.getTransaction();
            transaction.begin();
            sendResponse(packet);
        } else if (packet.operation.equals(ClusterOperation.TRANSACTION_COMMIT)) {
            Transaction transaction = node.factory.getTransaction();
            transaction.commit();
            sendResponse(packet);
        } else if (packet.operation.equals(ClusterOperation.TRANSACTION_ROLLBACK)) {
            Transaction transaction = node.factory.getTransaction();
            transaction.rollback();
            sendResponse(packet);
        } else if (packet.operation.equals(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS)) {
            IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            ConcurrentMapManager.Entries entries = (Entries) map.keySet();
            List list = entries.getLsKeyValues();
            Keys keys = new Keys();
            for (Object obj : list) {
                KeyValue entry = (KeyValue) obj;
                keys.addKey(entry.key);
            }
            packet.value = toData(keys);
            sendResponse(packet);
        } else if (packet.operation.equals(ClusterOperation.REMOTELY_PROCESS)) {
            node.clusterService.enqueueAndReturn(packet);
        } else if (packet.operation.equals(ClusterOperation.ADD_LISTENER)) {
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            Object key = toObject(packet.key);
            boolean includeValue = (int) packet.longValue == 1;
            if (key == null) {
                map.addEntryListener(clientEndpoint, includeValue);
            } else {
                map.addEntryListener(clientEndpoint, key, includeValue);
            }
            sendResponse(packet);
        }
        
        
    }

    private void sendResponse(Packet request) {
        request.operation = ClusterOperation.RESPONSE;
        request.responseType = RESPONSE_SUCCESS;
        if (request.conn != null && request.conn.live()) {
            request.conn.getWriteHandler().enqueuePacket(request);
        }
    }
}
