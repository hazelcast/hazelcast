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

import static com.hazelcast.impl.Constants.ExecutorOperations.OP_EXE_REMOTE_EXECUTION;
import static com.hazelcast.impl.Constants.ExecutorOperations.OP_STREAM;
import static com.hazelcast.impl.Constants.Objects.OBJECT_CANCELLED;
import static com.hazelcast.impl.Constants.Objects.OBJECT_DONE;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Timeouts.DEFAULT_TIMEOUT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.ClusterImpl.ClusterMember;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.InvocationQueue.Data;
import com.hazelcast.nio.InvocationQueue.Invocation;

class ExecutorManager extends BaseManager implements MembershipListener {

	static ExecutorManager instance = new ExecutorManager();

	private final ThreadPoolExecutor executor;

	private Map<RemoteExecutionId, SimpleExecution> mapRemoteExecutions = new ConcurrentHashMap<RemoteExecutionId, SimpleExecution>(
			1000);

	private Map<Long, DistributedExecutorAction> mapExecutions = new ConcurrentHashMap<Long, DistributedExecutorAction>(
			100);

	private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
	
	private ExecutorService eventFireExecutor = Executors.newSingleThreadExecutor();

	private BlockingQueue<Long> executionIds = new ArrayBlockingQueue<Long>(100);

	public static ExecutorManager get() {
		return instance;
	}

	private ExecutorManager() { 
		int corePoolSize = Config.get().executorConfig.corePoolSize;
		int maxPoolSize = Config.get().executorConfig.maxPoolsize;
		long keepAliveSeconds = Config.get().executorConfig.keepAliveSeconds;
		if (DEBUG) {
			log("Executor core:" + corePoolSize + ", max:" + maxPoolSize + ", keepAlive:"
					+ keepAliveSeconds);
		}
		executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveSeconds,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		Node.get().getClusterImpl().addMembershipListener(this);
		for (int i = 0; i < 100; i++) {
			executionIds.add(new Long(i));
		}
	}

	public void handle(Invocation inv) {
		if (inv.operation == OP_EXE_REMOTE_EXECUTION) {
			handleRemoteExecution(inv);
		} else if (inv.operation == OP_STREAM) {
			final StreamResponseHandler streamResponseHandler = mapStreams.get(inv.longValue);
			if (streamResponseHandler != null) {
				final Data value = inv.doTake(inv.data);
				executor.execute(new Runnable() {
					public void run() {
						streamResponseHandler.handleStreamResponse(value);
					}
				});
			}
			inv.returnToContainer();
		} else
			throw new RuntimeException("Unknown operation " + inv.operation);
	}

	public void handleRemoteExecution(Invocation inv) {
		if (DEBUG)
			log("Remote Handling invocaiton %%%%5555... " + inv);
		Data callableData = inv.doTake(inv.data);
		RemoteExecutionId remoteExecutionId = new RemoteExecutionId(inv.conn.getEndPoint(),
				inv.longValue);
		SimpleExecution se = new SimpleExecution(remoteExecutionId, executor, null, callableData,
				null, false);
		mapRemoteExecutions.put(remoteExecutionId, se);
		executor.execute(se);
		inv.returnToContainer();
	}

	public class RemoteExecutionId {
		public long executionId;
		public Address address;

		public RemoteExecutionId(Address address, long executionId) {
			super();
			this.address = address;
			this.executionId = executionId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((address == null) ? 0 : address.hashCode());
			result = prime * result + (int) (executionId ^ (executionId >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RemoteExecutionId other = (RemoteExecutionId) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			if (executionId != other.executionId)
				return false;
			return true;
		}
	}

	public void memberAdded(MembershipEvent membersipEvent) {
	}

	public void memberRemoved(final MembershipEvent membersipEvent) {
		Collection<DistributedExecutorAction> executionActions = mapExecutions.values();
		for (DistributedExecutorAction distributedExecutorAction : executionActions) {
			distributedExecutorAction.handleMemberLeft(membersipEvent.getMember());
		}
	}

	public class DistributedExecutorAction<T> implements Processable, ExecutionManagerCallback,
			StreamResponseHandler {
		InnerFutureTask<T> innerFutureTask = null;

		DistributedTask<T> distributedFutureTask = null;

		private final int expectedResultCount;

		private AtomicInteger resultCount = new AtomicInteger();

		protected volatile Data task = null;

		protected volatile Object callable = null;

		protected MemberImpl randomTarget = null;

		protected SimpleExecution simpleExecution = null; // for local tasks

		protected Long executionId = null;

		protected final BlockingQueue responseQueue = new LinkedBlockingQueue();

		protected boolean localOnly = true;

		public DistributedExecutorAction(Long executionId,
				DistributedTask<T> distributedFutureTask, Data task, Object callable, long timeout) {
			if (executionId == null)
				throw new RuntimeException("executionId cannot be null!");
			this.executionId = executionId;
			this.task = task;
			this.callable = callable;
			this.distributedFutureTask = distributedFutureTask;
			this.innerFutureTask = (InnerFutureTask<T>) distributedFutureTask.getInner();
			if (innerFutureTask.getMembers() != null) {
				expectedResultCount = innerFutureTask.getMembers().size();
			} else {
				expectedResultCount = 1;
			}
		}

		@Override
		public String toString() {
			return "ExecutorAction [" + executionId + "] expectedResultCount="
					+ expectedResultCount + ", resultCount=" + resultCount;
		}

		protected MemberImpl getTargetMember() {
			MemberImpl target = null;
			if (innerFutureTask.getKey() != null) {
				Data keyData = null;
				try {
					keyData = ThreadContext.get().toData(innerFutureTask.getKey());
				} catch (Exception e) {
					e.printStackTrace();
				}
				target = getKeyOwner(keyData);
			} else if (innerFutureTask.getMember() != null) {
				Object mem = innerFutureTask.getMember();
				if (mem instanceof ClusterMember) {
					ClusterMember clusterMember = (ClusterMember) mem;
					target = getMember(clusterMember.getAddress());
				} else if (mem instanceof MemberImpl) {
					target = (MemberImpl) mem;
				}
				if (DEBUG)
					log(" Target " + target);
			} else {
				int random = (int) (Math.random() * 100);
				target = lsMembers.get(random % lsMembers.size());
				randomTarget = target;
			}
			if (target == null)
				return clusterService.thisMember;
			else
				return target;
		}

		public void invoke() {
			clusterService.enqueueAndReturn(DistributedExecutorAction.this);
		}

		public void process() {
			mapExecutions.put(executionId, this);
			mapStreams.put(executionId, this);
			if (innerFutureTask.getMembers() == null) {
				MemberImpl memberTarget = getTargetMember();
				if (memberTarget.localMember()) {
					executeLocal();
				} else {
					localOnly = false;
					Invocation inv = obtainServiceInvocation("m:exe", null, task,
							OP_EXE_REMOTE_EXECUTION, DEFAULT_TIMEOUT);
					inv.timeout = DEFAULT_TIMEOUT;
					inv.longValue = executionId.longValue();
					boolean sent = send(inv, memberTarget.getAddress());
					if (!sent) {
						inv.returnToContainer();
						handleMemberLeft(memberTarget);
					}
				}
			} else {
				Set<Member> members = innerFutureTask.getMembers();
				for (Member member : members) {
					if (member.localMember()) {
						executeLocal();
					} else {
						localOnly = false;
						Invocation inv = obtainServiceInvocation("m:exe", null, task,
								OP_EXE_REMOTE_EXECUTION, DEFAULT_TIMEOUT);
						inv.timeout = DEFAULT_TIMEOUT;
						inv.longValue = executionId.longValue();
						boolean sent = send(inv, ((ClusterMember) member).getAddress());
						if (!sent) {
							inv.returnToContainer();
							handleMemberLeft(member);
						}
					}
				}
			}

		}

		void handleMemberLeft(Member member) {
			boolean found = false;
			ClusterMember clusterMember = (ClusterMember) member;
			if (innerFutureTask.getKey() != null) {
				Data keyData = ThreadContext.get().toData(innerFutureTask.getKey());
				MemberImpl target = getKeyOwner(keyData);
				if (clusterMember.getAddress().equals(target.getAddress())) {
					found = true;
				}
			} else if (innerFutureTask.getMember() != null) {
				Member target = innerFutureTask.getMember();
				if (target instanceof ClusterMember) {
					if (target.equals(member)) {
						found = true;
					}
				} else {
					if (((MemberImpl) target).getAddress().equals(clusterMember.getAddress())) {
						found = true;
					}
				}

			} else if (innerFutureTask.getMembers() != null) {
				Set<Member> members = innerFutureTask.getMembers();
				for (Member targetMember : members) {
					if (member.equals(targetMember)) {
						found = true;
					}
				}
			} else {
				if (clusterMember.getAddress().equals(randomTarget.getAddress())) {
					found = true;
				}
			}
			if (!found)
				return;
			innerFutureTask.innerSetMemberLeft(member);
			handleStreamResponse(OBJECT_DONE);

		}

		public void executeLocal() {
			simpleExecution = new SimpleExecution(null, executor, this, task, callable, true);
			executor.execute(simpleExecution);
		}

		/**
		 * !! Called by multiple threads.
		 */
		public void handleStreamResponse(Object response) {
			if (response == null)
				response = OBJECT_NULL;
			if (response == OBJECT_DONE || response == OBJECT_CANCELLED) {
				responseQueue.add(response);
				finalizeTask();
			} else {
				int resultCountNow = resultCount.incrementAndGet();
				if (resultCountNow >= expectedResultCount) {
					try {
						if (response != null)
							responseQueue.add(response);
						responseQueue.add(OBJECT_DONE);
					} catch (Exception e) {
						e.printStackTrace();
					}
					finalizeTask();
				} else {
					responseQueue.add(response);
				}
			}
		}

		public Object get(long timeout, TimeUnit unit) throws InterruptedException {
			try {
				Object result = (timeout == -1) ? responseQueue.take() : responseQueue.poll(
						timeout, unit);
				if (result == null)
					return null;
				if (result instanceof Data) {
					// returning the remote result
					// on the client thread
					return ThreadContext.get().getObjectReaderWriter().readObject((Data) result);
				} else {
					// local execution result
					return result;
				}
			} catch (InterruptedException e) {
				// if client thread is interrupted
				// finalize for clean interrupt..
				finalizeTask();
				throw e;
			} catch (Exception e) {
				// shouldn't happen..
				e.printStackTrace();
			}
			return null;
		}

		public Object get() throws InterruptedException {
			return get(-1, null);
		}

		/**
		 * !! Called by multiple threads.
		 */
		private void finalizeTask() {
			if (innerFutureTask != null)
				innerFutureTask.innerDone();
			// System.out.println("finalizing.. " + executionId);
			if (executionId == null)
				return;
			if (innerFutureTask.getExecutionCallback() != null) {
				innerFutureTask.getExecutionCallback().done(distributedFutureTask);
			}
			mapStreams.remove(executionId.longValue());
			Object action = mapExecutions.remove(executionId);
			// System.out.println("finalizing action  " + action);
			if (action != null) {
				boolean offered = executionIds.offer(executionId);
				if (!offered)
					throw new RuntimeException("Couldn't offer the executionId " + executionId);
			}
			executionId = null;
		}

		public boolean cancel(boolean mayInterruptIfRunning) {
			if (localOnly) {
				// local
				return simpleExecution.cancel(mayInterruptIfRunning);
			} else {
				// remote
				boolean cancelled = false;
				try {
					Callable<Boolean> callCancel = new CancelationTask(executionId.longValue(),
							thisAddress, mayInterruptIfRunning);
					if (innerFutureTask.getMembers() == null) {
						DistributedTask<Boolean> task = null;
						if (innerFutureTask.getKey() != null) {
							task = new DistributedTask<Boolean>(callCancel, innerFutureTask
									.getKey());
						} else if (innerFutureTask.getMember() != null) {
							task = new DistributedTask<Boolean>(callCancel, innerFutureTask
									.getMember());
						} else {
							task = new DistributedTask<Boolean>(callCancel, randomTarget);
						}
						Hazelcast.getExecutorService().execute(task);
						cancelled = task.get().booleanValue();
					} else {
						// members
						MultiTask<Boolean> task = new MultiTask<Boolean>(callCancel,
								innerFutureTask.getMembers());
						Hazelcast.getExecutorService().execute(task);
						Collection<Boolean> results = task.get();
						for (Boolean result : results) {
							if (result.booleanValue())
								cancelled = true;
						}
					}
				} catch (Throwable t) {
					cancelled = true;
				} finally {
					if (cancelled) {
						handleStreamResponse(OBJECT_CANCELLED);
					}
				}
				return cancelled;
			}
		}
	}

	public static class CancelationTask implements Callable<Boolean>, DataSerializable {
		long executionId = -1;
		Address address = null;
		boolean mayInterruptIfRunning = false;

		public CancelationTask() {
			super();
		}

		public CancelationTask(long executionId, Address address, boolean mayInterruptIfRunning) {
			super();
			this.executionId = executionId;
			this.address = address;
			this.mayInterruptIfRunning = mayInterruptIfRunning;
		}

		public Boolean call() {
			SimpleExecution simpleExecution = ExecutorManager.get().mapRemoteExecutions
					.remove(executionId);
			if (simpleExecution == null)
				return false;
			return simpleExecution.cancel(mayInterruptIfRunning);
		}

		public void readData(DataInput in) throws IOException {
			executionId = in.readLong();
			address = new Address();
			address.readData(in);
			mayInterruptIfRunning = in.readBoolean();
		}

		public void writeData(DataOutput out) throws IOException {
			out.writeLong(executionId);
			address.writeData(out);
			out.writeBoolean(mayInterruptIfRunning);
		}

	}

	public Processable createNewExecutionAction(DistributedTask task, long timeout) {
		if (task == null)
			throw new RuntimeException("task cannot be null");

		try {
			Long executionId = executionIds.take();
			DistributedTask dtask = task;
			InnerFutureTask inner = (InnerFutureTask) dtask.getInner();

			Callable callable = inner.getCallable();
			Data callableData = ThreadContext.get().toData(callable);
			DistributedExecutorAction action = new DistributedExecutorAction(executionId, dtask,
					callableData, callable, timeout);
			inner.setExecutionManagerCallback(action);
			return action;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void executeEventFireTask (EventFireTask eventFireTask) {
		eventFireExecutor.execute(eventFireTask);
	}

	public ScheduledExecutorService getScheduledExecutorService() {
		return scheduledExecutorService;
	}
	
	public void executeLocaly(Runnable runnable) {
		executor.execute(runnable);
	}

	private static class SimpleExecution implements Runnable {

		Data value = null;

		Object callable = null;

		ExecutorService executor = null;

		volatile boolean cancelled = false;

		volatile boolean done = false; // if done, don't interrupt

		volatile boolean running = false;

		Thread runningThread = null;

		DistributedExecutorAction action = null;

		boolean local = false;

		RemoteExecutionId remoteExecutionId;

		public SimpleExecution(RemoteExecutionId remoteExecutionId, ExecutorService executor,
				DistributedExecutorAction action, Data value, Object callable, boolean local) {
			super();
			this.value = value;
			this.callable = callable;
			this.executor = executor;
			this.action = action;
			this.local = local;
			this.remoteExecutionId = remoteExecutionId;
		}

		public static Object call(RemoteExecutionId remoteExecutionId, boolean local,
				Object callable) throws InterruptedException {
			Object executionResult = null;
			try {
				if (callable instanceof Callable) {
					executionResult = ((Callable) callable).call();
				} else if (callable instanceof Runnable) {
					((Runnable) callable).run();
				}
			} catch (InterruptedException e) {
				throw e;
			} catch (Throwable e) {
				// executionResult = new ExceptionObject(e);
				executionResult = e;
			} finally {
				if (!local) {
					ExecutorManager.get().mapRemoteExecutions.remove(remoteExecutionId);
				}
			}
			return executionResult;
		}

		public void run() {
			if (cancelled)
				return;
			Object executionResult = null;
			if (callable == null) {
				callable = ThreadContext.get().toObject(value);
				executor.execute(this);
			} else {
				runningThread = Thread.currentThread();
				running = true;
				try {
					executionResult = call(remoteExecutionId, local, callable);
				} catch (InterruptedException e) {
					cancelled = true;
				}
				if (cancelled)
					return;
				running = false;
				done = true; // should not be able to interrupt after this.
				if (!local) {
					try {
						ExecutorServiceProxy executorServiceProxy = (ExecutorServiceProxy) FactoryImpl
								.getExecutorService();
						executorServiceProxy.sendStreamItem(remoteExecutionId.address,
								executionResult, remoteExecutionId.executionId);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					// action.onResult(executionResult);
					action.handleStreamResponse(executionResult);
				}
			}
		}

		public boolean cancel(boolean mayInterruptIfRunning) {
			if (DEBUG) {
				System.out.println("SimpleExecution is cancelling..");
			}
			if (done || cancelled)
				return false;
			if (running && mayInterruptIfRunning)
				runningThread.interrupt();
			cancelled = true;
			return true;
		}
	}
}
