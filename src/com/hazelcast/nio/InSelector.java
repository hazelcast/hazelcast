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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.hazelcast.impl.Config;

public class InSelector extends SelectorBase {

	private static final InSelector instance = new InSelector();

	public static InSelector get() {
		return instance;
	}

	private ServerSocketChannel serverSocketChannel;

	SelectionKey key = null;

	public InSelector() {
		super();
		this.waitTime = 64;

	}

	public void init(ServerSocketChannel serverSocketChannel) {
		this.serverSocketChannel = serverSocketChannel;
		try {
			key = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, new Acceptor());
		} catch (ClosedChannelException e) {
			e.printStackTrace();
		}
		if (DEBUG) {
			System.out
					.println("Started Selector at " + serverSocketChannel.socket().getLocalPort());
		}
		selector.wakeup();
	}

	private class Acceptor implements SelectionHandler {

		public void handle() {
			try {
				SocketChannel channel = serverSocketChannel.accept(); 
				if (DEBUG)
					System.out.println(channel.socket().getLocalPort()
							+ " this socket is connected to "
							+ channel.socket().getRemoteSocketAddress());
				if (channel != null) {
					Connection connection = initChannel(channel, true);
					InetSocketAddress remoteSocket =  (InetSocketAddress) channel.socket().getRemoteSocketAddress();
					int remoteRealPort = Config.get().port + ((remoteSocket.getPort() - 10000) % 49);
					Address remoteAddress = new Address(remoteSocket.getAddress(), remoteRealPort);
					ConnectionManager.get().bind(remoteAddress, connection);
					channel.register(selector, SelectionKey.OP_READ, connection.getReadHandler());
				}
				serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, Acceptor.this);
				selector.wakeup();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					serverSocketChannel.close();
				} catch (Exception e1) {
				}
				ConnectionManager.get().shutdown();
			}
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();
		try {
			serverSocketChannel.close();
		} catch (IOException e) {
		}
	}

}
