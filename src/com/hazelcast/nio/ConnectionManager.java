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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import com.hazelcast.impl.Build;
import com.hazelcast.impl.ClusterManager;
import com.hazelcast.impl.Node;

public class ConnectionManager {

	private static final ConnectionManager instance = new ConnectionManager();

	public static ConnectionManager get() {
		return instance;
	}

	private ConnectionManager() {
	}

	private Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(
			100);

	private volatile boolean live = true;

	private Set<Address> setConnectionInProgress = new CopyOnWriteArraySet<Address>();

	private Set<ConnectionListener> setConnectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

	private boolean acceptTypeConnection = false;

	public synchronized Connection createConnection(SocketChannel socketChannel, boolean acceptor) {
		Connection connection = new Connection(socketChannel);
		try {
			if (acceptor) {
				// do nothing. you will be registering for the
				// write operation when you have something to
				// write already in the outSelector thread.
			} else {
				InSelector.get().addTask(connection.getReadHandler());
				// socketChannel.register(inSelector.selector,
				// SelectionKey.OP_READ, readHandler);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connection;
	}

	public void addConnectionListener(ConnectionListener listener) {
		setConnectionListeners.add(listener);
	}

	public Connection[] getConnections() {
		Object[] connObjs = mapConnections.values().toArray();
		Connection[] conns = new Connection[connObjs.length];
		for (int i = 0; i < conns.length; i++) {
			conns[i] = (Connection) connObjs[i];
		}
		return conns;
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
			if (setConnectionInProgress.add(address)) {
				if (!ClusterManager.get().shouldConnectTo(address))
					throw new RuntimeException("Should not connect to " + address);
				OutSelector.get().connect(address);
			}
		}
		return connection;
	}

	public synchronized void bind(Address endPoint, Connection connection, boolean accept) {
		connection.setEndPoint(endPoint);
		Connection connExisting = mapConnections.get(endPoint);
		if (connExisting != null && connExisting != connection) {
			if (Build.DEBUG) {
				final String msg = "Two connections from the same endpoint " + endPoint
						+ ", acceptTypeConnection=" + acceptTypeConnection + ",  now accept="
						+ accept;
				System.out.println(msg);
				ClusterManager.get().publishLog(msg);
			}
			return;
		}
		if (!endPoint.equals(Node.get().getThisAddress())) {
			acceptTypeConnection = accept;
			mapConnections.put(endPoint, connection);
			setConnectionInProgress.remove(endPoint);
			for (ConnectionListener listener : setConnectionListeners) {
				listener.connectionAdded(connection);
			}
		} else
			throw new RuntimeException("ConnMan setting self!!");
	}

	public synchronized void remove(Connection connection) {
		if (connection == null)
			return;
		if (connection.getEndPoint() != null) {
			mapConnections.remove(connection.getEndPoint());
			setConnectionInProgress.remove(connection.getEndPoint());
			for (ConnectionListener listener : setConnectionListeners) {
				listener.connectionRemoved(connection);
			}
		}
		if (connection.live())
			connection.close();
	}

	public synchronized void shutdown() {
		live = false;
		for (Connection conn : mapConnections.values()) {
			try {
				remove(conn);
			} catch (Exception e) {
			}
		}
		InSelector.get().shutdown();
		OutSelector.get().shutdown();
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
}
