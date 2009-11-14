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
import static com.hazelcast.impl.BaseManager.getInstanceType;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Instance;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Transaction;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.BaseManager.EventTask;
import com.hazelcast.impl.BaseManager.KeyValue;
import com.hazelcast.impl.ConcurrentMapManager.Entries;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl;
import com.hazelcast.impl.FactoryImpl.MProxy;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl.CollectionProxyReal;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.query.Predicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class ClientService {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new HashMap<Connection, ClientEndpoint>();
    private final ClientOperationHandler[] clientOperationHandlers = new ClientOperationHandler[300];

    public ClientService(Node node) {
        this.node = node;
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_PUT.getValue()] = new MapPutHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT.getValue()] = new MapPutIfAbsentHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_GET.getValue()] = new MapGetHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE.getValue()] = new MapRemoveHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE_IF_SAME.getValue()] = new MapRemoveIfSameHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_EVICT.getValue()] = new MapEvictHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REPLACE_IF_NOT_NULL.getValue()] = new MapReplaceIfNotNullHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REPLACE_IF_SAME.getValue()] = new MapReplaceIfSameHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_SIZE.getValue()] = new MapSizeHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_GET_MAP_ENTRY.getValue()] = new GetMapEntryHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_LOCK.getValue()] = new MapLockHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_UNLOCK.getValue()] = new MapUnlockHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_CONTAINS.getValue()] = new MapContainsHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE.getValue()] = new MapContainsValueHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_BEGIN.getValue()] = new TransactionBeginHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_COMMIT.getValue()] = new TransactionCommitHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_ROLLBACK.getValue()] = new TransactionRollbackHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS.getValue()] = new MapIterateKeysHandler();
        clientOperationHandlers[ClusterOperation.ADD_LISTENER.getValue()] = new AddListenerHandler();
        clientOperationHandlers[ClusterOperation.REMOVE_LISTENER.getValue()] = new RemoveListenerHandler();
        clientOperationHandlers[ClusterOperation.REMOTELY_PROCESS.getValue()] =  new RemotelyProcessHandler();
        clientOperationHandlers[ClusterOperation.DESTROY.getValue()] =  new DestroyHandler();
        clientOperationHandlers[ClusterOperation.GET_ID.getValue()] =  new GetIdHandler();
        clientOperationHandlers[ClusterOperation.ADD_INDEX.getValue()] =  new AddIndexHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ADD_TO_LIST.getValue()] = new ListAddHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM.getValue()] = new ListRemoveHandler();
    }

    // always called by InThread
    public void handle(Packet packet) {
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, packet, callContext, clientOperationHandlers);
        if(!packet.operation.equals(ClusterOperation.CONCURRENT_MAP_UNLOCK)){
        	node.clusterManager.enqueueEvent(clientEndpoint.hashCode(), clientRequestHandler);
        }
        else{
        	node.executorManager.executeMigrationTask(clientRequestHandler);
        }
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
        final private Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>();
        final Map<String, Map<Object, EntryEvent>> listeneds = new HashMap<String, Map<Object, EntryEvent>>();

        ClientEndpoint(Connection conn) {
            this.conn = conn;
        }

        public CallContext getCallContext(int threadId) {
            CallContext context = callContexts.get(threadId);
            if (context == null) {
                int locallyMappedThreadId = ThreadContext.get().createNewThreadId();
                context = new CallContext(locallyMappedThreadId, true);
                callContexts.put(threadId, context);
            }
            return context;
        }
        
        public void addThisAsListener(IMap map, Data key, boolean includeValue){
        	if (key == null) {
                map.addEntryListener(this, includeValue);
            } else {
                map.addEntryListener(this, key, includeValue);
            }
        }

		private Map<Object, EntryEvent> getEventProcessedLog(String name) {
			Map<Object, EntryEvent> eventProcessedLog = listeneds.get(name);
        	if(eventProcessedLog == null){
        		synchronized (name) {
        			if(eventProcessedLog == null){
        				eventProcessedLog = new HashMap<Object, EntryEvent>();
        				listeneds.put(name, eventProcessedLog);
        			}
				}
        	}
        	return eventProcessedLog;
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
        	Map<Object, EntryEvent> eventProcessedLog = getEventProcessedLog(event.getName());
        	if(eventProcessedLog.get(event.getKey())!=null && eventProcessedLog.get(event.getKey()) == event){
        		return;
        	}
        	eventProcessedLog.put(event.getKey(), event);
        	Object key = listeneds.get(event.getName());
        	Packet packet = createEntryEventPacket(event);
        	sendPacket(packet);
        }

        private void sendPacket(Packet packet) {
            if (conn != null && conn.live()) {
                conn.getWriteHandler().enqueuePacket(packet);
            }
        }

        private Packet createEntryEventPacket(EntryEvent event) {
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
    
    
    
    

    private class RemotelyProcessHandler extends ClientOperationHandler{
		public void processCall(Node node, Packet packet) {
			node.clusterService.enqueueAndReturn(packet);
		}
		@Override
		protected void sendResponse(Packet request) {
		}
    }
    private class DestroyHandler extends ClientOperationHandler{
		public void processCall(Node node, Packet packet) {
			Instance instance = (Instance)node.factory.getOrCreateProxyByName(packet.name);
			instance.destroy();
		}
    }
    private class GetIdHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return toData(map.getId());
		}
    }
    private class AddIndexHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			map.addIndex((String)toObject(key), (Boolean)toObject(value));
			return null;
		}
    }
    private class MapPutHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			Object oldValue = map.put(key, value);
            return (oldValue==null)?null:(Data) oldValue;
		}
    }
    private class MapPutIfAbsentHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			Object oldValue = map.putIfAbsent(key, value);
            return (oldValue==null)?null:(Data) oldValue;
		}
    }
    private class MapGetHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return (Data) map.get(key);
		}
    }
    private class MapRemoveHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return (Data) map.remove(key);
		}
    }
    private class MapRemoveIfSameHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return toData(map.remove(key, value));
		}
    }
    private class MapEvictHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return toData(map.evict(key));
		}
    }
    private class MapReplaceIfNotNullHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return (Data)map.replace(key, value);
		}
    }
    private class MapReplaceIfSameHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			Object[] arr = (Object[]) toObject(value);
			return toData(map.replace(key, arr[0], arr[1]));
		}
    }
    private class MapContainsHandler extends ClientOperationHandler{
		public void processCall(Node node, Packet packet) {
			if(getInstanceType(packet.name).equals(InstanceType.MAP)){
				IMap<Object, Object> map = (IMap)node.factory.getOrCreateProxyByName(packet.name);
				packet.value = toData(map.containsKey(packet.key));
			}
			else if (getInstanceType(packet.name).equals(InstanceType.LIST)){
				IList<Object> list = (IList)node.factory.getOrCreateProxyByName(packet.name);
				packet.value = toData(list.contains(packet.key));
			}
		}
    }
    private class MapContainsValueHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return toData(map.containsValue(value));
		}
    }
    private class MapSizeHandler extends ClientOperationHandler{
		@Override
		public void processCall(Node node, Packet packet) {
			if(getInstanceType(packet.name).equals(InstanceType.MAP)){
				IMap<Object, Object> map = (IMap)node.factory.getOrCreateProxyByName(packet.name);
				packet.value = toData(map.size());
			}
			else if(getInstanceType(packet.name).equals(InstanceType.LIST)){
				IList<Object> list = (IList)node.factory.getOrCreateProxyByName(packet.name);
				packet.value = toData(list.size());
			}
			
		}
    }
    private class GetMapEntryHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			return toData(map.getMapEntry(key));
		}
    }
    private class MapLockHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			throw new RuntimeException("Shouldn't invoke this method");
		}
		@Override
		public void processCall(Node node, Packet packet){
    		IMap<Object, Object> map = (IMap)node.factory.getOrCreateProxyByName(packet.name);
    		long timeout = packet.timeout;
    		Data value = null;
    		if(timeout==-1){
    			map.lock(packet.key);
    			value = null;
    		}
    		else if(timeout == 0){
    			value = toData(map.tryLock(packet.key));
    		}
    		else{
    			value = toData(map.tryLock(packet.key, timeout,(TimeUnit) toObject(packet.value)));
    		}
    		packet.value = value;
    	}
    }
    private class MapUnlockHandler extends ClientMapOperationHandler{
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
			map.unlock(key);
			return null;
		}
    }
    private class TransactionBeginHandler extends ClientTransactionOperationHandler {
		public void processTransactionOp(Transaction transaction) {
			transaction.begin();
		}
    }
    private class TransactionCommitHandler extends ClientTransactionOperationHandler {
		public void processTransactionOp(Transaction transaction) {
			transaction.commit();
		}
    }
    private class TransactionRollbackHandler extends ClientTransactionOperationHandler {
		public void processTransactionOp(Transaction transaction) {
			transaction.rollback();
		}
    }
    private class MapIterateKeysHandler extends ClientOperationHandler {
		public Data processMapOp(IMap<Object, Object> map, Data key, Data value, Collection<Data> collection) {
			ConcurrentMapManager.Entries entries = null;
			if(value==null){
				entries = (Entries) map.keySet();				
			}
			else{
				Predicate p = (Predicate)toObject(value);
				entries = (Entries) map.keySet(p);
			}
            List<Map.Entry> list = entries.getKeyValues();
            Keys keys = new Keys(collection);
            for (Object obj : list) {
                KeyValue entry = (KeyValue) obj;
                keys.addKey(entry.key);
            }
            return toData(keys);
		}

		@Override
		public void processCall(Node node, Packet packet) {
			if(getInstanceType(packet.name).equals(InstanceType.MAP)){
				packet.value = processMapOp((IMap)node.factory.getOrCreateProxyByName(packet.name), packet.key, packet.value, new HashSet<Data>());
			}
			else if(getInstanceType(packet.name).equals(InstanceType.LIST)){
				CollectionProxyImpl collectionProxy = (CollectionProxyImpl)node.factory.getOrCreateProxyByName(packet.name);
    			MProxy mapProxy  = ((CollectionProxyReal)collectionProxy.getBase()).mapProxy;
    			packet.value = processMapOp(mapProxy, packet.key, packet.value, new ArrayList<Data>());
			}
			
		}

    }
    private class AddListenerHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
    		boolean includeValue = (int) packet.longValue == 1;
    		if(getInstanceType(packet.name).equals(InstanceType.MAP)){
    			IMap<Object, Object> map = (IMap)node.factory.getOrCreateProxyByName(packet.name);
    			clientEndpoint.addThisAsListener(map, packet.key, includeValue);
    		}
    		else if(getInstanceType(packet.name).equals(InstanceType.LIST)){
    			CollectionProxyImpl collectionProxy = (CollectionProxyImpl)node.factory.getOrCreateProxyByName(packet.name);
    			MProxy mapProxy  = ((CollectionProxyReal)collectionProxy.getBase()).mapProxy;
    			mapProxy.addEntryListener(clientEndpoint, includeValue);
    		}
    	}
    }
    private class RemoveListenerHandler extends ClientOperationHandler {
    	public void processCall(Node node, Packet packet) {
    		ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            IMap<Object, Object> map = (IMap)node.factory.getOrCreateProxyByName(packet.name);
            if (packet.key == null) {
                map.removeEntryListener(clientEndpoint);
            } else {
                map.removeEntryListener(clientEndpoint, packet.key);
            }
    	}
    }
    
    private class ListAddHandler extends ClientOperationHandler{
		@Override
		public void processCall(Node node, Packet packet) {
			IList list = (IList)node.factory.getOrCreateProxyByName(packet.name);
			packet.value = toData(list.add(packet.key));
		}
    }
    
    private class ListRemoveHandler extends ClientOperationHandler{
		@Override
		public void processCall(Node node, Packet packet) {
			IList list = (IList)node.factory.getOrCreateProxyByName(packet.name);
			packet.value = toData(list.remove(packet.key));
		}
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
    private abstract class ClientMapOperationHandler extends ClientOperationHandler{
    	public abstract Data processMapOp(IMap<Object, Object> map, Data key, Data value);
    	public void processCall(Node node, Packet packet){
    		IMap<Object, Object> map = (IMap)node.factory.getOrCreateProxyByName(packet.name);
    		Data value = processMapOp(map,packet.key, packet.value);
    		packet.value = value;
    	}
		
    }
    private abstract class ClientTransactionOperationHandler extends ClientOperationHandler{
    	public abstract void processTransactionOp(Transaction transaction);
    	public void processCall(Node node, Packet packet){
    		Transaction transaction = node.factory.getTransaction();
    		processTransactionOp(transaction);
    	}	
    }
}
