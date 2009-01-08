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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.hazelcast.impl.ClusterManager;
import com.hazelcast.impl.Config;
import com.hazelcast.impl.Node;

public class OutSelector extends SelectorBase {

	private static final OutSelector instance = new OutSelector();
	private Set<Integer> boundPorts = new HashSet<Integer>();

	public static OutSelector get() {
		return instance;
	}

	private OutSelector() {
		super();
		super.waitTime = 1;
	}

	public void connect(Address address) {
		if (DEBUG)
			System.out.println("connect to " + address);
		Connector connector = new Connector(address);
		this.addTask(connector);

	}

	private class Connector implements Runnable, SelectionHandler {
		Address address;

		SocketChannel socketChannel = null;

		int localPort = 0;

		int numberOfConnectionError = 0;

		public Connector(Address address) {
			super();
			this.address = address;
		}

		public void run() {
			try {
				socketChannel = SocketChannel.open();
				Address thisAddress = Node.get().getThisAddress();
				int addition = (thisAddress.getPort() - Config.get().port);
				localPort = 10000 + addition;
				boolean bindOk = false;
				while (!bindOk) {
					try {
						localPort += 20;
						if (boundPorts.size() > 1000 || localPort > 60000) {
							boundPorts.clear();
							Connection[] conns = ConnectionManager.get().getConnections();
							for (Connection conn : conns) {
								// conn is live or not, assume it is bounded
								boundPorts.add(conn.localPort);
							}
						}
						if (boundPorts.add(localPort)) {
							socketChannel.socket().bind(
									new InetSocketAddress(thisAddress.getInetAddress(), localPort));
							bindOk = true;
							socketChannel.configureBlocking(false);
							if (DEBUG)
								System.out.println("connecting to " + address);
							socketChannel.connect(new InetSocketAddress(address.getInetAddress(),
									address.getPort()));
						}

					} catch (Exception e) {
						// ignore
					}
				}
				socketChannel.register(selector, SelectionKey.OP_CONNECT, Connector.this);
			} catch (Exception e) {
				try {
					socketChannel.close();
				} catch (IOException ignored) { 
				}
				if (numberOfConnectionError++ < 5) {
					if (DEBUG) {
						System.out.println("Couldn't register connect! will trying again. cause: " + e.getMessage());
					}
					run();
				} else {
					ConnectionManager.get().failedConnection(address);
				}
			}
		}

		public void handle() {
			try {
				boolean finished = socketChannel.finishConnect();
				if (!finished) {
					socketChannel.register(selector, SelectionKey.OP_CONNECT, Connector.this);
					return;
				}
				if (DEBUG) {
					System.out.println("connected to " + address);
				}
				Connection connection = initChannel(socketChannel, false);
				connection.localPort = localPort;
				ConnectionManager.get().bind(address, connection, false);
			} catch (Exception e) {
				try {
					if (DEBUG) {
						String msg = "Couldn't connect to " + address + ", cause: "
								+ e.getMessage();
						System.out.println(msg);
						ClusterManager.get().publishLog(msg);
						e.printStackTrace();
					}
					socketChannel.close();
					if (numberOfConnectionError++ < 5) {
						if (DEBUG) {
							System.out.println("Couldn't finish connecting, will try again. cause: " + e.getMessage());
						}
						addTask(Connector.this);
					} else {
						ConnectionManager.get().failedConnection(address);
					}
				} catch (Exception ignored) {
				}
			}
		}

	}

}
