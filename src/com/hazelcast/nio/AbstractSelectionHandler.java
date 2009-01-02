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

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.hazelcast.impl.Build;
import com.hazelcast.impl.ClusterManager;

public abstract class AbstractSelectionHandler implements SelectionHandler {
	public static final boolean DEBUG = Build.DEBUG;

	protected SocketChannel socketChannel;

	protected InSelector inSelector;

	protected OutSelector outSelector;

	protected Connection connection; 

	protected SelectionKey sk = null;

	public AbstractSelectionHandler(Connection connection) {
		super();
		this.connection = connection;
		this.socketChannel = connection.getSocketChannel();
		this.inSelector = InSelector.get();
		this.outSelector = OutSelector.get();
	}
	
	final void registerOp(Selector selector, int operation) {
		try {
			if (!connection.live())
				return;
			if (sk == null) {
				sk = socketChannel.register(selector, operation, this);
			} else {
				sk.interestOps(operation);
			}  
		} catch (Exception e) {
			handleSocketException(e);
		}
	}

	final void handleSocketException(Exception e) {
		if (DEBUG) {
			System.out.println(Thread.currentThread().getName() + " Closing Socket. cause:  " + e);
		} 
		if (DEBUG) {
			e.printStackTrace();
		}
		if (sk != null) sk.cancel();
		if (connection.live()) {
			if (DEBUG) {
				ClusterManager.get().publishLog("Connection.close endPoint:" + connection.getEndPoint() + ", cause" + e.toString());						
			}
		}
		connection.close();
	}

	protected void shutdown() {

	}

}
