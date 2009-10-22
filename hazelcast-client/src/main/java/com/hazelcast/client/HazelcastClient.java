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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.client.core.IMap;
import com.hazelcast.client.core.Transaction;
import com.hazelcast.client.impl.ListenerManager;

public class HazelcastClient {
	final Map<Long,Call> calls  = new ConcurrentHashMap<Long, Call>();
	final ListenerManager listenerManager;
	final OutRunnable out;
	final InRunnable in;
	final ConnectionManager connectionManager;
	
	private HazelcastClient(InetSocketAddress[] clusterMembers) {
		connectionManager = new ConnectionManager(this, clusterMembers);

		out = new OutRunnable(this, calls, new PacketWriter());
		new Thread(out,"hz.client.OutThread").start();
		
		in = new InRunnable(this, calls, new PacketReader());
		new Thread(in,"hz.client.InThread").start();

		listenerManager = new ListenerManager();
		new Thread(listenerManager,"hz.client.Listener").start();
		
		
		try {
			connectionManager.getConnection();
		} catch (IOException ignored) {
		}
	}

	public static HazelcastClient getHazelcastClient(InetSocketAddress... clusterMembers){
		return new HazelcastClient(clusterMembers);
	}
	
	
	

	public <K, V> IMap<K,V> getMap(String name){
		MapClientProxy<K, V> proxy = new MapClientProxy<K, V>(this,name);
		proxy.setOutRunnable(out);
		return proxy;
	}


	public Transaction getTransaction() {
		ThreadContext trc = ThreadContext.get();
		TransactionClientProxy proxy = (TransactionClientProxy)trc.getTransaction();
		proxy.setOutRunnable(out);
		return proxy;
	}

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}
	
	public void shutdown(){
		out.shutdown();
		listenerManager.shutdown();
		in.shutdown();
	}
}
