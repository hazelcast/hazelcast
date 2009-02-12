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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.impl.Node;
import com.hazelcast.nio.InvocationQueue.Invocation;

public class WriteHandler extends AbstractSelectionHandler implements Runnable {

	protected static Logger logger = Logger.getLogger(WriteHandler.class.getName());

	public static final boolean DEBUG = false;

	private final BlockingQueue writeHandlerQueue = new LinkedBlockingQueue();

	private final static ByteBuffer bbOut = ByteBuffer.allocateDirect(1024 * 1024); 

	private final AtomicBoolean informSelector = new AtomicBoolean(true);

	boolean ready = false;

	boolean dead = false; 

	WriteHandler(final Connection connection) {
		super(connection);
	}

	public final void enqueueInvocation(final Invocation inv) {
		try {
			writeHandlerQueue.put(inv); 
		} catch (final InterruptedException e) {
			Node.get().handleInterruptedException(Thread.currentThread(), e);
		}
		if (informSelector.get()) {
			informSelector.set(false);
			outSelector.addTask(this);
			if (inv.currentCallCount < 2) { 
				outSelector.selector.wakeup();
			}
		}
	}

	public final void handle() { 
		if (writeHandlerQueue.size() == 0) {
			ready = true;
			return;
		}
		if (!connection.live())
			return;
		try {
			bbOut.clear();
			copyLoop: while (bbOut.position() < (32 * 1024)) {
				final Invocation inv = (Invocation) writeHandlerQueue.poll();
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
					final int written = socketChannel.write(bbOut);
					remaining -= written;
					loopCount++;
					if (DEBUG) {
						if (loopCount > 1) {
							logger.log(Level.INFO, "loopcount " + loopCount);
						}
					}
				} catch (final Exception e) {
					handleSocketException(e);
					return;
				}
			}
		} catch (final Throwable t) {
			logger.log(Level.INFO, "Fatal Error: WriteHandler " + t);
		} finally {
			ready = false;
			registerWrite();
		}
	}

	public final void run() {
		informSelector.set(true);
		if (ready) {
			handle();
		} else {
			registerWrite();
		}
		ready = false;
	}

	private final void registerWrite() {
		registerOp(outSelector.selector, SelectionKey.OP_WRITE);
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

	private final void doPostWrite(final Invocation inv) {
		if (inv.container != null) {
			try {
				inv.container.returnInvocation(inv);
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}
}
