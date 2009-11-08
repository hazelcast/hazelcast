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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import com.hazelcast.core.IMap;
import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.MultiMap;

public class HazelcastClient implements HazelcastInstance{
	private static final String MAP_PREFIX = "c:";
	private static final String LIST_PREFIX = "m:l:";
	private static final String SET_PREFIX = "m:s:";
	final Map<Long,Call> calls  = new ConcurrentHashMap<Long, Call>();
	final ListenerManager listenerManager;
	final OutRunnable out;
	final InRunnable in;
	final ConnectionManager connectionManager;
	final Map<String, ClientProxy> mapProxies = new ConcurrentHashMap<String, ClientProxy>(100); 
	
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
		return (IMap<K,V>)getClientProxy(MAP_PREFIX + name);
	}

	private <K, V, E> ClientProxy getClientProxy(String name) {
		ClientProxy proxy = mapProxies.get(name);
		if(proxy==null){
			synchronized (mapProxies) {
				if(proxy==null){
					if(name.startsWith(MAP_PREFIX)){
						proxy = new MapClientProxy<K, V>(this,name);
					}
					else if(name.startsWith(LIST_PREFIX)){
						proxy = new ListClientProxy<E>(this, name);
					}
					else if(name.startsWith(SET_PREFIX)){
						proxy = new SetClientProxy<E>(this, name);
					}
					proxy.setOutRunnable(out);
					mapProxies.put(name, proxy);
				}
			}
		}
		return mapProxies.get(name);
	}


	public com.hazelcast.core.Transaction getTransaction() {
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

	public void addInstanceListener(InstanceListener instanceListener) {
		// TODO Auto-generated method stub
		
	}

	public Cluster getCluster() {
		// TODO Auto-generated method stub
		return null;
	}

	public ExecutorService getExecutorService() {
		// TODO Auto-generated method stub
		return null;
	}

	public IdGenerator getIdGenerator(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public Collection<Instance> getInstances() {
		// TODO Auto-generated method stub
		return null;
	}

	public <E> IList<E> getList(String name) {
		return (IList<E>)getClientProxy(LIST_PREFIX + name);
	}

	public ILock getLock(Object obj) {
		// TODO Auto-generated method stub
		return null;
	}

	public <K, V> MultiMap<K, V> getMultiMap(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	public <E> IQueue<E> getQueue(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public <E> ISet<E> getSet(String name) {
		return (ISet<E>)getClientProxy(SET_PREFIX + name);
	}

	public <E> ITopic<E> getTopic(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public void removeInstanceListener(InstanceListener instanceListener) {
		// TODO Auto-generated method stub
		
	}

	public void restart() {
		// TODO Auto-generated method stub
	}
	
	protected void destroy(String proxyName){
		mapProxies.remove(proxyName);
	}
}
