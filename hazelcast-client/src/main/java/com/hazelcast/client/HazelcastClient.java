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
import com.hazelcast.config.Config;

/***
 * Hazelcast Client enables you to do all Hazelcast operations wihtout being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it. When the relied cluster member
 * dies, client will transparently switch to another live member.
 */
public class HazelcastClient implements HazelcastInstance{
	private static final String MAP_PREFIX = "c:";
	private static final String LIST_PREFIX = "m:l:";
	private static final String SET_PREFIX = "m:s:";
    private static final String QUEUE_PREFIX = "q:";
    private static final String TOPIC_PREFIX = "t:";
    private static final String IDGEN_PREFIX = "i:";
    private static final String MULTIMAP_PROXY = "m:u:";
    

    final Map<Long,Call> calls  = new ConcurrentHashMap<Long, Call>();
    final ListenerManager listenerManager;
    final OutRunnable out;
    final InRunnable in;
    final ConnectionManager connectionManager;
    final Map<Object, ClientProxy> mapProxies = new ConcurrentHashMap<Object, ClientProxy>(100);
    final ExecutorServiceManager executorServiceManager;
    final IMap mapLockProxy;
    final ClusterClientProxy clusterClientProxy;
    final String groupName;

    private HazelcastClient(String groupName, String groupPassword, boolean shuffle, InetSocketAddress[] clusterMembers) {
        connectionManager = new ConnectionManager(this, clusterMembers, shuffle);
        this.groupName = groupName;
		out = new OutRunnable(this, calls, new PacketWriter());
		new Thread(out,"hz.client.OutThread").start();

		in = new InRunnable(this, calls, new PacketReader());
		new Thread(in,"hz.client.InThread").start();

		listenerManager = new ListenerManager(this);
		new Thread(listenerManager,"hz.client.Listener").start();

		try {
			connectionManager.getConnection();
		} catch (IOException ignored) {
		}

        executorServiceManager = new ExecutorServiceManager(this);
        new Thread(executorServiceManager,"hz.client.executorManager").start();

        mapLockProxy = getMap("__hz_Locks");

        clusterClientProxy = new ClusterClientProxy(this);
        clusterClientProxy.setOutRunnable(out);
        Boolean authenticate = clusterClientProxy.authenticate(groupName, groupPassword);
        if(!authenticate){
            this.shutdown();
            throw new RuntimeException("Wrong group name and password.");
        }

	}

    /***
     * Returns a new HazelcastClient. It will shuffle the given address list and pick one address to connect.
     *
     * @param groupName Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param addresses Addresses of Cluster Members that client will choose one to connect. If the connected member
     * dies client will switch to the next one in the list.
     * An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     * ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, String... addresses){
        return newHazelcastClient(groupName, groupPassword, true, addresses);
    }

    /***
     * Returns a new HazelcastClient.
     *
     * @param groupName Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param shuffle Specifies whether to shuffle the list of addresses
     * @param addresses Addresses of Cluster Members that client will choose one to connect. If the connected member
     * dies client will switch to the next one in the list.
     * An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
     * ex: "10.90.0.1", "10.90.0.2:5702"
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, boolean shuffle, String... addresses){
        InetSocketAddress[] socketAddressArr = new InetSocketAddress[addresses.length];

        for(int i=0;i<addresses.length;i++){
            String[] seperated = addresses[i].split(":");
            int port = (seperated.length>1)?Integer.valueOf(seperated[1]):5701;

            InetSocketAddress inetSocketAddress = new InetSocketAddress(seperated[0], port);
            socketAddressArr[i] = inetSocketAddress;
        }
        return newHazelcastClient(groupName, groupPassword, shuffle, socketAddressArr);
    }

    /***
     * Returns a new HazelcastClient.
     *
     * @param groupName Group name of a cluster that client will connect
     * @param groupPassword Group Password of a cluster that client will connect.
     * @param shuffle Specifies whether to shuffle the list of addresses
     * @param addresses InetSocketAddress of Cluster Members that client will choose one to connect. If the connected member
     * dies client will switch to the next one in the list.
     * @return Returns a new Hazelcast Client instance.
     */
    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, boolean shuffle, InetSocketAddress... addresses){
        return new HazelcastClient(groupName, groupPassword, shuffle, addresses);
    }
	
	public Config getConfig() {
        throw new UnsupportedOperationException();
    }
	

	public <K, V> IMap<K,V> getMap(String name){
		return (IMap<K,V>)getClientProxy(MAP_PREFIX + name);
	}


	public <K, V, E> ClientProxy getClientProxy(Object o) {
		ClientProxy proxy = mapProxies.get(o);
		if(proxy==null){
			synchronized (mapProxies) {
                proxy = mapProxies.get(o);
				if(proxy==null){
                    if(o instanceof String){
                        String name = (String)o;
                        if(name.startsWith(MAP_PREFIX)){
                            proxy = new MapClientProxy<K, V>(this,name);
                        }
                        else if(name.startsWith(LIST_PREFIX)){
                            proxy = new ListClientProxy<E>(this, name);
                        }
                        else if(name.startsWith(SET_PREFIX)){
                            proxy = new SetClientProxy<E>(this, name);
                        }
                        else if(name.startsWith(QUEUE_PREFIX)){
                            proxy = new QueueClientProxy<E>(this, name);
                        }
                        else if(name.startsWith(TOPIC_PREFIX)){
                            proxy = new TopicClientProxy<E>(this, name);
                        }
                        else if(name.startsWith(IDGEN_PREFIX)){
                            proxy = new IdGeneratorClientProxy(this, name);
                        }
                        else if(name.startsWith(MULTIMAP_PROXY)){
                            proxy = new MultiMapClientProxy(this, name);
                        }
                        else{
                            proxy = new LockClientProxy(o, this);
                        }
					proxy.setOutRunnable(out);
					mapProxies.put(o, proxy);
                    }
				}
			}
		}
		return mapProxies.get(o);
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
        executorServiceManager.shutdown();
	}

	public void addInstanceListener(InstanceListener instanceListener) {
		clusterClientProxy.addInstanceListener(instanceListener);
    }

	public Cluster getCluster() {
        return clusterClientProxy;
    }

	public ExecutorService getExecutorService() {
		return new ExecutorServiceClientProxy(this);
	}

	public IdGenerator getIdGenerator(String name) {
		return (IdGenerator)getClientProxy(IDGEN_PREFIX + name);
	}

	public Collection<Instance> getInstances() {
		return clusterClientProxy.getInstances();
	}

	public <E> IList<E> getList(String name) {
		return (IList<E>)getClientProxy(LIST_PREFIX + name);
	}

	public ILock getLock(Object obj) {
        return new LockClientProxy(obj, this);
	}

	public <K, V> MultiMap<K, V> getMultiMap(String name) {
		return (MultiMap<K,V>)getClientProxy(MULTIMAP_PROXY + name);
	}

	public String getName() {
		return groupName;
	}

	public <E> IQueue<E> getQueue(String name) {
		return (IQueue<E>)getClientProxy(QUEUE_PREFIX + name);
	}

	public <E> ISet<E> getSet(String name) {
		return (ISet<E>)getClientProxy(SET_PREFIX + name);
	}

	public <E> ITopic<E> getTopic(String name) {
		// TODO Auto-generated method stub
		return (ITopic)getClientProxy(TOPIC_PREFIX + name);
	}

	public void removeInstanceListener(InstanceListener instanceListener) {
        clusterClientProxy.removeInstanceListener(instanceListener);
	}

	public void restart() {
        throw new UnsupportedOperationException();
    }
	
	protected void destroy(String proxyName){
		mapProxies.remove(proxyName);
	}
}
