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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
	PacketWriter writer;
	PacketReader reader;
	Connection connection;
	private List<InetSocketAddress> clusterMembers = new ArrayList<InetSocketAddress>();
	
	private HazelcastClient(InetSocketAddress[] clusterMembers) {
		this.clusterMembers.addAll(Arrays.asList(clusterMembers));
		Collections.shuffle(this.clusterMembers);
		writer = new PacketWriter();
		reader = new PacketReader();
		
		connection = searchForAvailableConnection();
		writer.setConnection(connection);
		reader.setConnection(connection);

		out = new OutRunnable();
		out.setCallMap(calls);
		out.setPacketWriter(writer);
		new Thread(out).start();
		
		in = new InRunnable(this, reader);
		in.setCallMap(calls);
		new Thread(in).start();
		
		sendBindRequest(connection.getAddress(), connection);
				
		new Thread(listenerManager).start();
	}

	public static HazelcastClient getHazelcastClient(InetSocketAddress... clusterMembers){
		return new HazelcastClient(clusterMembers);
	}
	
	private void sendBindRequest(InetSocketAddress address,
			Connection connection) {
		Bind b = null;
		try {
			b = new Bind(new Address(address.getHostName(),connection.getSocket().getLocalPort()));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		Packet bind = new Packet();
		bind.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, Serializer.toByte(null), Serializer.toByte(b));
		Call cBind = new Call();
		cBind.setRequest(bind);
		cBind.setId(Call.callIdGen.incrementAndGet());
		out.enQueue(cBind);
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

	public void attachedClusterMemeberConnectionLost() {
		System.out.println("The connection to member : "+ clusterMembers.get(0) + " is lost");
		popAndPush(clusterMembers);
		
		connection = searchForAvailableConnection();
		sendBindRequest(connection.getAddress(), connection);
		reader.setConnection(connection);
		writer.setConnection(connection);
		notifyWaitingCalls();
	}

	private void popAndPush(List<InetSocketAddress> clusterMembers) {
		InetSocketAddress address =clusterMembers.remove(0); 
		clusterMembers.add(address);
	}

	private Connection searchForAvailableConnection() {
		Connection connection =null;
		int counter = clusterMembers.size();
		while(counter>0){
			try{
				connection = getNextConnection();
				break;
			}catch(Exception e){
				popAndPush(clusterMembers);
				counter--;
			}
		}
		if(counter == 0){
			throw new RuntimeException("No cluster member available to connect");
		}
		System.out.println("Returning connection: " + connection.getAddress());
		return connection;
	}

	private Connection getNextConnection(){
		InetSocketAddress address = clusterMembers.get(0);
		Connection connection  = new Connection(address);
		return connection;
	}

	private void notifyWaitingCalls() {
		Collection<Call> cc = calls.values();
		List<Call> removed = new ArrayList<Call>();
		removed.addAll(cc);
		for (Iterator<Call> iterator = removed.iterator(); iterator.hasNext();) {
			Call call =  iterator.next();
			calls.remove(call.getId());
			synchronized (call) {
				call.notify();
			}
//			out.enQueue(call);
		}
	}
	
	public Connection getCurrentConnection(){
		return connection;
	}

}
