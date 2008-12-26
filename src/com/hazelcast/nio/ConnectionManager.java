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

package com.hazelcast.nio;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import com.hazelcast.impl.ClusterService;
import com.hazelcast.impl.Constants;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.ClusterManager.AddRemoveConnection;
import com.hazelcast.nio.InvocationQueue.Invocation;

public class ConnectionManager {

	private static final ConnectionManager instance = new ConnectionManager();

	public static ConnectionManager get() {
		return instance;
	}

	private ConnectionManager() {
	}

	private Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(100);
 
	private volatile boolean live = true;

	public Set<Address> setConnectionInProgress = new CopyOnWriteArraySet<Address>();

	public synchronized Connection createConnection(SocketChannel socketChannel, boolean acceptor) {
		Connection connection = new Connection(socketChannel, Node.get());
		WriteHandler writeHandler = new WriteHandler(connection);
		ReadHandler readHandler = new ReadHandler(connection);
		connection.setHandlers(writeHandler, readHandler);
		try {
			if (acceptor) {
				// do nothing. you will be registering for the
				// write operation when you have something to
				// write already in the outSelector thread.
			} else {
				InSelector.get().addTask(readHandler);
				// socketChannel.register(inSelector.selector,
				// SelectionKey.OP_READ, readHandler);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connection;
	}
	
	public Connection[] getConnections() {
		return (Connection[]) mapConnections.values().toArray();
	}

	public Connection getConnection(Address address) {
		return mapConnections.get(address);
	}

	public synchronized void failedConnection(Address address) {
		setConnectionInProgress.remove(address);
		if (!Node.get().joined()) {
			Node.get().failedConnection(address);
		}
	}

	public synchronized Connection getOrConnect(Address address) {
		if (address.equals(Node.get().getThisAddress()))
			throw new RuntimeException("Connecting to self! " + address);
		Connection connection = mapConnections.get(address);
		if (connection == null) {
			try {
				if (!setConnectionInProgress.contains(address)) {
					setConnectionInProgress.add(address);
					OutSelector.get().connect(address);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return connection;
	}

	public synchronized void finalizeAndBind(Connection connection) {
		try {
			Address address = connection.getEndPoint();
			setConnectionInProgress.remove(address);
			if (!address.equals(Node.get().getThisAddress())) {
				mapConnections.put(address, connection); 
				ClusterService.get().enqueueAndReturn(new AddRemoveConnection(address, true));
			}
			Invocation invBind = InvocationQueue.instance().obtainInvocation();
			invBind.set("bind", Constants.ClusterOperations.OP_BIND, null, Node.get()
					.getThisAddress());
			connection.getWriteHandler().writeInvocation(invBind);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized String toString() {
		StringBuffer sb = new StringBuffer("Connections {");
		for (Connection conn : mapConnections.values()) {
			sb.append("\n" + conn);
		}
		sb.append("\nlive=" + live);
		sb.append("\n}");
		return sb.toString();
	}

	public synchronized void setConnection(Address endPoint, Connection connection) {
		if (!endPoint.equals(Node.get().getThisAddress())) {
			mapConnections.put(endPoint, connection); 
		} else
			throw new RuntimeException("ConnMan setting self!!");
	}

	public synchronized void remove(Connection connection) {
		if (connection == null)
			return;
		if (connection.getEndPoint() != null) {
			mapConnections.remove(connection.getEndPoint()); 
		}
		if (connection.live())
			connection.close();

	}

	public synchronized void shutdown() {
		live = false;
		InSelector.get().shutdown();
		OutSelector.get().shutdown();
		for (Connection conn : mapConnections.values()) {
			remove(conn);
		}

	}

}
