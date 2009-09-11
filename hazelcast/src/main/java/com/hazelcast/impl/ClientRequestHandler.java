package com.hazelcast.impl;


import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BaseManager.KeyValue;
import com.hazelcast.impl.ConcurrentMapManager.Entries;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;

import static com.hazelcast.nio.BufferUtil.*;

import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;

import static com.hazelcast.core.Transaction.*;

public class ClientRequestHandler implements Runnable{
	private Packet packet;
	public ClientRequestHandler(Packet request) {
		this.packet = request;
	}

	public void run() {
		CallContext callContext = packet.conn.getCallContext(packet.threadId);
		ThreadContext.get().setCallContext(callContext);
		ThreadContext.get().setClient(true);
		if(packet.operation.equals(ClusterOperation.CONCURRENT_MAP_PUT)){
			IMap<Object, Object> map = Hazelcast.getMap(packet.name.substring(2));
			Object oldValue = map.put(doHardCopy(packet.key), doHardCopy(packet.value));
			doSet((Data)oldValue, packet.value);
			sendResponse(packet);
		}
		else if(packet.operation.equals(ClusterOperation.CONCURRENT_MAP_GET)){
			IMap<Object, Object> map = Hazelcast.getMap(packet.name.substring(2));
			Object value = map.get(doHardCopy(packet.key));
			Data data = (Data)value;
			if(callContext.txn!=null && callContext.txn.getStatus() == TXN_STATUS_ACTIVE){ 
				data = doHardCopy(data);
			}
			doSet(data, packet.value);
			sendResponse(packet);
		}
		else if(packet.operation.equals(ClusterOperation.TRANSACTION_BEGIN)){
			Transaction transaction = Hazelcast.getTransaction();
			transaction.begin();
			sendResponse(packet);
		}
		else if(packet.operation.equals(ClusterOperation.TRANSACTION_COMMIT)){
			Transaction transaction = Hazelcast.getTransaction();
			transaction.commit();
			sendResponse(packet);
		}
		else if(packet.operation.equals(ClusterOperation.TRANSACTION_ROLLBACK)){
			Transaction transaction = Hazelcast.getTransaction();
			transaction.rollback();
			sendResponse(packet);
		}
		else if(packet.operation.equals(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS)){
			IMap<Object, Object> map = Hazelcast.getMap(packet.name.substring(2));
			ConcurrentMapManager.Entries entries = (Entries) map.keySet();
			List list = entries.getLsKeyValues();
			Keys keys = new Keys();
			for (Object obj : list) {
				KeyValue entry = (KeyValue)obj;
				keys.addKey(entry.key);
			} 
			doSet(toData(keys), packet.value);
			sendResponse(packet);
		}
		

	}

	private void sendResponse(Packet request) {
		request.operation = ClusterOperation.RESPONSE;
		request.responseType = RESPONSE_SUCCESS;
		if(request.conn != null && request.conn.live()){
			request.conn.getWriteHandler().enqueuePacket(request);
		}
	}
	

}
