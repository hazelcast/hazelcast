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

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.hazelcast.impl.Node;

public class OutSelector extends SelectorBase {

	private static final OutSelector instance = new OutSelector();

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
		if (Node.get().getThisAddress().equals(address))
			throw new RuntimeException("Connecting to self! " + address);
		Connector connector = new Connector(address);
		this.addTask(connector);

	}

	private class Connector implements Runnable, SelectionHandler {
		Address address;

		SocketChannel socketChannel = null;

		public Connector(Address address) {
			super();
			this.address = address;
		}

		public void run() {
			try {
				socketChannel = SocketChannel.open();
				Address thisAddress = Node.get().getThisAddress();
				socketChannel.socket().bind(new InetSocketAddress(thisAddress.getInetAddress(), 0));
				socketChannel.configureBlocking(false);
				if (DEBUG)
					System.out.println("connecting to " + address);
				socketChannel.connect(new InetSocketAddress(address.getInetAddress(), address
						.getPort()));
				socketChannel.register(selector, SelectionKey.OP_CONNECT, Connector.this);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void handle() {
			try {
				boolean finished = socketChannel.finishConnect();
				if (!finished)
					throw new RuntimeException("Couldn't finish connection to " + address);
				if (DEBUG)
					System.out.println("connected to " + address);
				Connection connection = initChannel(socketChannel, false);
				connection.setEndPoint(address);
				ConnectionManager.get().finalizeAndBind(connection); 
			} catch (Exception e) {
				if (DEBUG) {
					System.out.println("Couldn't connect to " + address);
					e.printStackTrace();
				}
				if (!Node.get().joined()) {
					Node.get().failedConnection(address);
				}
			}
		}

	}

}
