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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.impl.Build;
import com.hazelcast.impl.ClusterManager;
import com.hazelcast.impl.Node;

public class SelectorBase implements Runnable {

	protected static final boolean DEBUG = Build.get().DEBUG;

	protected Selector selector = null;

	protected BlockingQueue<Runnable> selectorQueue = new ArrayBlockingQueue<Runnable>(10000);

	protected boolean live = true;

	protected int waitTime = 16;

	AtomicInteger size = new AtomicInteger();

	public SelectorBase() {
		try {
			selector = Selector.open();
		} catch (IOException e) {
			handleSelectorException(e);
		}
	}

	protected Connection initChannel(SocketChannel socketChannel, boolean acceptor)
			throws Exception {
		socketChannel.socket().setReceiveBufferSize(32 * 1024);
		socketChannel.socket().setSendBufferSize(64 * 1024);
		socketChannel.socket().setKeepAlive(true);
		// socketChannel.socket().setTcpNoDelay(true);
		socketChannel.configureBlocking(false);
		Connection connection = ConnectionManager.get().createConnection(socketChannel, acceptor);
		return connection;
	}

	public void processSelectionQueue() {
		while (live) {
			Runnable runnable = selectorQueue.poll();
			if (runnable == null)
				return;
			runnable.run();
			size.decrementAndGet();
		}
	}

	public int addTask(Runnable runnable) {
		try {
			selectorQueue.put(runnable);
			return size.incrementAndGet();
		} catch (InterruptedException e) {
			Node.get().handleInterruptedException(Thread.currentThread(), e);
			return 0;
		}
	}

	public void run() {
		select: while (live) {
			if (size.get() > 0) {
				processSelectionQueue();
			}
			int selectedKeys = 0;
			try {
				selectedKeys = selector.select(waitTime);
				if (Thread.interrupted()) {
					Node.get().handleInterruptedException(Thread.currentThread(),
							new RuntimeException());
				}
			} catch (IOException ioe) {
				// normally select should never throw an exception
				// operation. If happens, continue selecting...
				ioe.printStackTrace();
				continue select;
			}
			if (selectedKeys == 0) {
				continue select;
			}
			Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
			Iterator<SelectionKey> it = setSelectedKeys.iterator();
			while (it.hasNext()) {
				SelectionKey sk = (SelectionKey) it.next();
				it.remove();
				try {
					sk.interestOps(sk.interestOps() & ~sk.readyOps());
					SelectionHandler selectionHandler = (SelectionHandler) sk.attachment();
					selectionHandler.handle();
				} catch (Exception e) {
					handleSelectorException(e);
				}
			}
		}
	}

	protected void handleSelectorException(Exception e) {
		String msg = "Selector exception at  " + Thread.currentThread().getName();
		msg += ", cause= " + e.toString();
		if (Build.DEBUG) {
			System.out.println(msg);
			ClusterManager.get().publishLog(msg);
			e.printStackTrace(System.out);
		}
	}

	public void shutdown() {
		live = false;
		selectorQueue.clear();
		try {
			selector.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
