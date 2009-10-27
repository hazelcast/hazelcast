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

import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BaseManager.EventTask;
import com.hazelcast.impl.BaseManager.KeyValue;
import com.hazelcast.impl.ConcurrentMapManager.Entries;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ClientService {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new HashMap<Connection, ClientEndpoint>();
    private ClientOperationHandler[] clientOperationHandlers = new ClientOperationHandler[300];

    public ClientService(Node node) {
        this.node = node;
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_PUT.getValue()] = new MapPutHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_GET.getValue()] = new MapGetHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_BEGIN.getValue()] = new TransactionBeginHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_COMMIT.getValue()] = new TransactionCommitHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_ROLLBACK.getValue()] = new TransactionRollbackHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS.getValue()] = new MapIterateKeysHandler();
        clientOperationHandlers[ClusterOperation.ADD_LISTENER.getValue()] = new AddListenerHandler();
        clientOperationHandlers[ClusterOperation.REMOTELY_PROCESS.getValue()] =  new RemotelyProcessHandler();
    }

    // always called by InThread
    public void handle(Packet packet) {
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, packet, callContext, clientOperationHandlers); 
        node.clusterManager.enqueueEvent(clientEndpoint.hashCode(), clientRequestHandler);
    }

    public ClientEndpoint getClientEndpoint(Connection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(conn);
            mapClientEndpoints.put(conn, clientEndpoint);
        }
        return clientEndpoint;
    }

    class ClientEndpoint implements EntryListener {
        final Connection conn;
        private Map<Integer, CallContext> mapOfCallContexts = new HashMap<Integer, CallContext>();

        ClientEndpoint(Connection conn) {
            this.conn = conn;
        }

        public CallContext getCallContext(int threadId) {
            CallContext context = mapOfCallContexts.get(threadId);
            if (context == null) {
                int locallyMappedThreadId = ThreadContext.get().createNewThreadId();
                context = new CallContext(locallyMappedThreadId, true);
                mapOfCallContexts.put(threadId, context);
            }
            return context;
        }
        
        @Override
        public int hashCode() {
            return this.conn.hashCode();
        }

        public void entryAdded(EntryEvent event) {
            processEvent(event);
        }

        public void entryEvicted(EntryEvent event) {
            processEvent(event);
        }

        public void entryRemoved(EntryEvent event) {
            processEvent(event);
        }

        public void entryUpdated(EntryEvent event) {
            processEvent(event);
        }

        private void processEvent(EntryEvent event) {
            Packet packet = createEventPacket(event);
            sendPacket(packet);
        }

        private void sendPacket(Packet packet) {
            if (conn != null && conn.live()) {
                conn.getWriteHandler().enqueuePacket(packet);
            }
        }

        private Packet createEventPacket(EntryEvent event) {
            Packet packet = new Packet();
            EventTask eventTask = (EventTask) event;
            packet.set(event.getName(), ClusterOperation.EVENT, eventTask.getDataKey(), eventTask.getDataValue());
            packet.longValue = event.getEventType().getType();
            return packet;
        }
        
    }

    public void reset() {
        mapClientEndpoints.clear();
    }
    
    public abstract class ClientOperationHandler{
    	public abstract void processCall(Node node, Packet packet);
    	public void handle(Node node, Packet packet){
    		processCall(node,packet);
    		sendResponse(packet);
    	}
    	
    	protected void sendResponse(Packet request) {
            request.operation = ClusterOperation.RESPONSE;
            request.responseType = RESPONSE_SUCCESS;
            if (request.conn != null && request.conn.live()) {
                request.conn.getWriteHandler().enqueuePacket(request);
            }
        }
    }
    public class RemotelyProcessHandler extends ClientOperationHandler{

		public void processCall(Node node, Packet packet) {
			node.clusterService.enqueueAndReturn(packet);
		}
		@Override
		protected void sendResponse(Packet request) {
			
		}
    }
    
    public class MapPutHandler extends ClientOperationHandler{
		public void processCall(Node node, Packet packet) {
			IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            Object oldValue = map.put(packet.key, packet.value);
            packet.value = (oldValue==null)?null:(Data) oldValue;
		}
    }
    public class MapGetHandler extends ClientOperationHandler{
		public void processCall(Node node, Packet packet) {
			IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            packet.value = (Data) map.get(packet.key);
            packet.key = null;
		}
    	
    }
    public class TransactionBeginHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		Transaction transaction = node.factory.getTransaction();
            transaction.begin();
    	}
    }
    public class TransactionCommitHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		Transaction transaction = node.factory.getTransaction();
            transaction.commit();
    	}
    }
    public class TransactionRollbackHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		Transaction transaction = node.factory.getTransaction();
            transaction.rollback();
    	}
    }
    public class MapIterateKeysHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            ConcurrentMapManager.Entries entries = (Entries) map.keySet();
            List<?> list = entries.getLsKeyValues();
            Keys keys = new Keys();
            for (Object obj : list) {
                KeyValue entry = (KeyValue) obj;
                keys.addKey(entry.key);
            }
            packet.value = toData(keys);
    	}
    }
    public class AddListenerHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            IMap<Object, Object> map = node.factory.getMap(packet.name.substring(2));
            Object key = toObject(packet.key);
            boolean includeValue = (int) packet.longValue == 1;
            if (key == null) {
                map.addEntryListener(clientEndpoint, includeValue);
            } else {
                map.addEntryListener(clientEndpoint, key, includeValue);
            }
    	}
    }
}
