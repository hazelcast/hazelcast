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

package com.hazelcast.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.Member;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.InvocationQueue.Invocation;

public class ClusterService implements Runnable, Constants {
	private static final ClusterService instance = new ClusterService();

	public static ClusterService get() {
		return instance;
	}

	private static final long PERIODIC_CHECK_INTERVAL = TimeUnit.SECONDS.toNanos(1);

	private static final long UTILIZATION_CHECK_INTERVAL = TimeUnit.SECONDS.toNanos(10);
 
	protected LinkedList<MemberImpl> lsMembers = new LinkedList<MemberImpl>();

	protected Address thisAddress = null;

	protected MemberImpl thisMember = null;

	protected int thisMemberIndex = -1;

	protected final boolean DEBUG = Build.get().DEBUG;

	protected MemberImpl nextMember = null;

	protected final BlockingQueue queue;

	protected volatile boolean running = true;

	protected final List lsBuffer = new ArrayList(2000);
	
	protected long start = 0;
	
	protected long totalProcessTime = 0;
	
	protected long lastPeriodicCheck = 0;

	private ClusterService() {
		this.queue = new LinkedBlockingQueue();
		this.thisAddress = Node.get().getThisAddress();
		this.start = System.nanoTime();
	}

	public void process(Object obj) {
		long processStart = System.nanoTime();
		if (obj instanceof Invocation) {
			Invocation inv = (Invocation) obj;
			MemberImpl memberFrom = getMember(inv.conn.getEndPoint());
			if (memberFrom != null) {
				memberFrom.didRead();
			}
			int operation = inv.operation;
			if (operation < 50) {
				ClusterManager.get().handle(inv);
			} else if (operation < 300) {
				ListenerManager.get().handle(inv);
			} else if (operation < 400) {
				ExecutorManager.get().handle(inv);
			} else if (operation < 500) {
				BlockingQueueManager.get().handle(inv);
			} else if (operation < 600) {
				ConcurrentMapManager.get().handle(inv);
			} else
				throw new RuntimeException("Unknown operation " + operation);
		} else if (obj instanceof Processable) {
			((Processable) obj).process();
		} else if (obj instanceof Runnable) {
			synchronized (obj) {
				((Runnable) obj).run();
				obj.notify();
			}
		} else
			throw new RuntimeException("Unkown obj " + obj);
		long processEnd = System.nanoTime();
		long elipsedTime = processEnd - processStart;
		totalProcessTime += elipsedTime;
		long duration = (processEnd - start);
		if (duration > UTILIZATION_CHECK_INTERVAL) {
			if (DEBUG) { 
				System.out.println("ServiceProcessUtilization: " + ((totalProcessTime * 100) / duration) + " %");				
			}
			start = processEnd;
			totalProcessTime = 0;
		}
	}

	public void run3() {
		Object obj = null;
		while (running) {
			try {
				obj = queue.take();
				process(obj);
			} catch (InterruptedException e) {
				Node.get().handleInterruptedException(Thread.currentThread(), e);
			} catch (Exception e) {
				if (DEBUG) {
					System.out.println(e + ",  message: " + e.getMessage() + "  obj=" + obj);
				}
				e.printStackTrace();
			}
		}
	}
	
	private final void checkPeriodics() {
		long now = System.nanoTime();
		if ((now - lastPeriodicCheck) > PERIODIC_CHECK_INTERVAL) { 
			ClusterManager.get().heartBeater();
			ClusterManager.get().checkScheduledActions();
			lastPeriodicCheck = now;
		}
	}

	public void run() {
		while (running) {
			Object obj = null;
			try {
				lsBuffer.clear();
				queue.drainTo(lsBuffer);
				int size = lsBuffer.size();
				if (size > 0) {
					for (int i = 0; i < size; i++) {
						obj = lsBuffer.get(i); 
						checkPeriodics();
						process(obj);
					}
					lsBuffer.clear();
				} else {
					obj = queue.poll(100, TimeUnit.MILLISECONDS);
					checkPeriodics();
					if (obj != null) {
						process(obj);
					} 
				}
			} catch (InterruptedException e) {
				Node.get().handleInterruptedException(Thread.currentThread(), e);
			} catch (Throwable e) {
				if (DEBUG) {
					System.out.println(e + ",  message: " + e + ", obj=" + obj);
				}
				e.printStackTrace(System.out);
			}
		}
	}

	public void stop() {
		this.running = false;
	}

	protected int getMemberCount() {
		return lsMembers.size();
	}

	protected void setNextMember() {
		for (Member m : lsMembers) {
			if (m.localMember())
				thisMemberIndex = lsMembers.indexOf(m);
		}
		nextMember = getNextMember();
	}

	protected int getThisMemberIndex() {
		return thisMemberIndex;
	}

	protected MemberImpl getNextMember() {
		if (lsMembers.size() < 2)
			return null;
		else {
			return lsMembers.get((thisMemberIndex + 1) % lsMembers.size());
		}
	}

	protected MemberImpl getPreviousMember() {
		if (lsMembers.size() < 2)
			return null;
		else {
			if (thisMemberIndex == 0)
				return lsMembers.get(lsMembers.size() - 1);
			else
				return lsMembers.get((thisMemberIndex - 1) % lsMembers.size());
		}
	}

	protected MemberImpl getNextMemberAfter(Address address) {
		MemberImpl member = getMember(address);
		if (member != null) {
			int indexOfMember = lsMembers.indexOf(member);
			return lsMembers.get((indexOfMember + 1) % lsMembers.size());
		} else
			return null;
	}

	protected MemberImpl getNextMemberAfter(List<MemberImpl> lsMembers, Address address) {
		int indexOfMember = getIndexOf(lsMembers, address);
		if (indexOfMember == -1)
			return null;
		return lsMembers.get((indexOfMember + 1) % lsMembers.size());
	}

	protected int getIndexOf(List<MemberImpl> lsMembers, Address address) {
		for (int i = 0; i < lsMembers.size(); i++) {
			MemberImpl member = lsMembers.get(i);
			if (member.getAddress().equals(address)) {
				return i;
			}
		}
		return -1;
	}

	public void enqueueAndReturn(Object message) {
		try {
			// if (DEBUG) {
			// if (queue.size() > 600) {
			// System.out.println("queue size " + queue.size());
			// }
			// }
			queue.put(message);
		} catch (InterruptedException e) {
			Node.get().handleInterruptedException(Thread.currentThread(), e);
		}
	}

	protected Member addMember(MemberImpl member) {
		if (DEBUG) {
			System.out.println("ClusterService adding " + member);
		}
		if (lsMembers.contains(member)) {
			for (MemberImpl m : lsMembers) {
				if (m.equals(member))
					member = m;
			}
		} else {
			if (!member.getAddress().equals(thisAddress)) {
				ConnectionManager.get().getConnection(member.getAddress());
			}
			lsMembers.add(member);
		}
		setNextMember();
		return member;
	}

	protected void removeMember(Address address) {
		if (DEBUG)
			System.out.println("removing  " + address);
		Member member = getMember(address);
		if (member != null) {
			lsMembers.remove(member);
		}
		setNextMember();
	}

	protected MemberImpl createMember(Address address) {
		MemberImpl member = new MemberImpl(address);
		if (address == thisAddress || address.equals(thisAddress)) {
			member.setThisMember(true);
			thisMember = member;
		} else
			member.setThisMember(false);
		return member;
	}

	protected MemberImpl getMember(Address address) {
		for (MemberImpl m : lsMembers) {
			if (m.getAddress().equals(address)) {
				return m;
			}
		}
		return null;
	}

	protected boolean isThisNextMaster() {
		MemberImpl nextMasterMember = getNextMemberAfter(Node.get().getMasterAddress());
		if (nextMasterMember == null)
			return false;
		return (nextMasterMember.localMember());
	}

	final boolean send(Invocation inv, Address address) {
		Connection conn = ConnectionManager.get().getConnection(address);
		if (conn == null)
			return false;
		if (!conn.live())
			return false;
		writeInvocation(conn, inv);
		return true;
	}

	final boolean send(Invocation inv, Connection conn) {
		if (conn != null) {
			writeInvocation(conn, inv);
		} else {
			return false;
		}
		return true;
	}

	final private void writeInvocation(Connection conn, Invocation inv) {
		if (!conn.live()) {
			inv.returnToContainer();
			return;
		}
		MemberImpl memberImpl = getMember(conn.getEndPoint());
		if (memberImpl != null) {
			memberImpl.didWrite();
		}
		inv.write();
		conn.getWriteHandler().enqueueInvocation(inv);
	}

	final public void addMember(Address address) {
		if (address == null) {
			if (DEBUG) {
				System.out.println("Address cannot be null");
				return;
			}
		}
		MemberImpl member = getMember(address);
		if (member == null)
			member = createMember(address);
		addMember(member);
	}

	@Override
	public String toString() {
		return "ClusterService queueSize=" + queue.size() + " master= " + Node.get().master()
				+ " master= " + Node.get().getMasterAddress();
	}

}
