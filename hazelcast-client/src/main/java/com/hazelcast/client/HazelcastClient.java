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

package com.hazelcast.client;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.client.cluster.Bind;
import com.hazelcast.client.core.IMap;
import com.hazelcast.client.core.Transaction;
import com.hazelcast.client.impl.ClusterOperation;
import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.client.nio.Address;

public class HazelcastClient {
	Map<Long,Call> calls  = new HashMap<Long, Call>();
	ListenerManager listenerManager = new ListenerManager();
	OutRunnable out;
	InRunnable in;
	private HazelcastClient(ClusterConfig config) {
		Connection connection = new Connection(config);

		out = new OutRunnable();
		out.setCallMap(calls);
		PacketWriter writer = new PacketWriter();
		writer.setConnection(connection);
		out.setPacketWriter(writer);
		new Thread(out).start();
		
		PacketReader reader = new PacketReader();
		reader.setConnection(connection);
		in = new InRunnable(this, reader);
		in.setCallMap(calls);
		new Thread(in).start();
		
		Bind b = null;
		try {
			b = new Bind(new Address(config.getHost(),connection.getSocket().getLocalPort()));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		Packet bind = new Packet();
		bind.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, Serializer.toByte(null), Serializer.toByte(b));
		Call cBind = new Call();
		cBind.setRequest(bind);
		cBind.setId(Call.callIdGen.incrementAndGet());
		out.enQueue(cBind);
		
		new Thread(listenerManager).start();
		
	}
	
	
	public static HazelcastClient getHazelcastClient(ClusterConfig config){
		return new HazelcastClient(config);
	}
	public <K, V> IMap<K,V> getMap(String name){
		MapClientProxy<K, V> proxy = new MapClientProxy<K, V>(this,name);
		proxy.setOutRunnable(out);
		return proxy;
	}


	public Transaction getTransaction() {
		TransactionClientProxy proxy = new TransactionClientProxy();
		proxy.setOutRunnable(out);
		return proxy;
	}
	
	

}
