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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.impl.ClusterService;
import com.hazelcast.impl.Node;
import com.hazelcast.nio.InvocationQueue.Invocation;

public class WriteHandler extends AbstractSelectionHandler implements Runnable {
	public static final boolean DEBUG = false;

	private BlockingQueue writeHandlerQueue = new ArrayBlockingQueue(500);

	List<Invocation> lsQueueBulk = new ArrayList<Invocation>(50);

	static ByteBuffer bbOut = ByteBuffer.allocateDirect(1024 * 1024);

	boolean dead = false;

	public WriteHandler(Connection connection) {
		super(connection);
	}

	public int getQueueSize() {
		return writeHandlerQueue.size();
	}

	@Override
	public void shutdown() {
		dead = false;
		rollback: while (writeHandlerQueue.size() > 0) {
			Invocation inv = (Invocation) writeHandlerQueue.poll();
			if (inv == null)
				break rollback;
			if (inv.local) {
				ClusterService.get().rollbackInvocation(inv);
			}
		}
		writeHandlerQueue.clear();
		lsWrittenLocals.clear();
	}

	AtomicBoolean alreadyRegistered = new AtomicBoolean(false);

	public void writeInvocation(Invocation inv) {
		try {
			if (!connection.live()) {
				ClusterService.get().rollbackInvocation(inv);
				return;
			}
			inv.write();

			writeHandlerQueue.put(inv);
		} catch (InterruptedException e) {
			Node.get().handleInterruptedException(Thread.currentThread(), e);
		}
		signalWriteRequest();
	}

	public void signalWriteRequest() {
		if (!alreadyRegistered.get()) {
			int size = outSelector.addTask(this);
			if (size < 5) {
				outSelector.selector.wakeup();
			}
		}
	}

	public void run() {
		registerWrite();
	}

	public void registerWrite() {
		try {
			if (sk == null) {
				sk = socketChannel.register(outSelector.selector, SelectionKey.OP_WRITE, this);
			} else {
				sk.interestOps(SelectionKey.OP_WRITE);
			}
			alreadyRegistered.set(true);
		} catch (Exception e) {
			handleSocketException(e);
		}
	}

	private void doPostWrite(Invocation inv) {
		if (inv.container != null) {
			try { 
				inv.container.returnInvocation(inv);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	List<Invocation> lsWrittenLocals = new ArrayList<Invocation>(100);

	public void handle() {
		try {
			bbOut.clear();
			lsWrittenLocals.clear();
			int totalC = 0;
			copyLoop: while (bbOut.position() < (32 * 1024)) {
				Invocation inv = (Invocation) take();
				if (inv == null)
					break copyLoop;
				inv.write(bbOut); 
				if (inv.local) {
					lsWrittenLocals.add(inv);
				}
				doPostWrite(inv);
				totalC++;
			}
			if (bbOut.position() == 0)
				return;
			bbOut.flip();
			// if (totalC > 30) System.out.println("total C " + totalC);
			int remaining = bbOut.remaining();
			int loopCount = 0;
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
					while (lsWrittenLocals.size() > 0) {
						writeHandlerQueue.add(lsWrittenLocals.remove(0));
					}
					handleSocketException(e);
				}
			}

		} finally {
			if (hasMore()) {
				registerWrite();
			} else {
				alreadyRegistered.set(false);
			}
		}
	}

	public boolean hasMore() {
		return (lsQueueBulk.size() > 0) || (writeHandlerQueue.size() > 0);
	}

	private Object take() {
		if (lsQueueBulk.size() == 0)
			writeHandlerQueue.drainTo(lsQueueBulk, 50);

		if (lsQueueBulk.size() > 0)
			return lsQueueBulk.remove(0);
		else
			return null;
	}

}
