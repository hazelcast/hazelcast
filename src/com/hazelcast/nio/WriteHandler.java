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

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.impl.Node;
import com.hazelcast.nio.InvocationQueue.Invocation;

public class WriteHandler extends AbstractSelectionHandler implements Runnable {
	public static final boolean DEBUG = false;

	private BlockingQueue writeHandlerQueue = new LinkedBlockingQueue();

	static ByteBuffer bbOut = ByteBuffer.allocateDirect(1024 * 1024);

	boolean dead = false;

	AtomicBoolean alreadyRegistered = new AtomicBoolean(false);

	WriteHandler(Connection connection) {
		super(connection);
	}

	@Override
	public void shutdown() {
		dead = false;
		Invocation inv = (Invocation) writeHandlerQueue.poll();
		while (inv != null) {
			inv.returnToContainer();
			inv = (Invocation) writeHandlerQueue.poll();
		}
		writeHandlerQueue.clear();
	}

	public final void enqueueInvocation(Invocation inv) {
		try {
			writeHandlerQueue.put(inv);
		} catch (InterruptedException e) {
			Node.get().handleInterruptedException(Thread.currentThread(), e);
		}
		signalWriteRequest();
	}

	final void signalWriteRequest() {
		if (!alreadyRegistered.get()) {
			int size = outSelector.addTask(this);
			if (size < 5) {
				outSelector.selector.wakeup();
			}
		}
	}

	public final void run() {
		registerWrite();
	}

	private final void registerWrite() {
		registerOp(outSelector.selector, SelectionKey.OP_WRITE);
		alreadyRegistered.set(true);
	}

	private final void doPostWrite(Invocation inv) {
		if (inv.container != null) {
			try {
				inv.container.returnInvocation(inv);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public final void handle() {
		if (!connection.live())
			return;
		try {
			bbOut.clear();
			copyLoop: while (bbOut.position() < (32 * 1024)) {
				Invocation inv = (Invocation) writeHandlerQueue.poll();
				if (inv == null)
					break copyLoop;
				inv.write(bbOut);
				doPostWrite(inv);
			}
			if (bbOut.position() == 0)
				return;
			bbOut.flip();
			int remaining = bbOut.remaining();
			int loopCount = 0;
			connection.didWrite();
			while (remaining > 0) {
				try {
					int written = socketChannel.write(bbOut);
					remaining -= written;
					loopCount++;
					if (DEBUG) {
						if (loopCount > 1) {
							System.out.println("loopcount " + loopCount);
						}
					}
				} catch (Exception e) {
					handleSocketException(e);
					return;
				}
			}
		} catch (Throwable t) {
			System.out.println("Fatal Error: WriteHandler " + t);
		} finally {
			if (hasMore()) {
				registerWrite();
			} else {
				alreadyRegistered.set(false);
				if (hasMore()) { // double check!
					registerWrite();
				}
			}
		}
	}

	private final boolean hasMore() {
		return (writeHandlerQueue.size() > 0);
	}

}
