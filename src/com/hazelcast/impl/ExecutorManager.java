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

import com.hazelcast.cluster.ClusterImpl.ClusterMember;
import com.hazelcast.cluster.ClusterManager;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import static com.hazelcast.impl.Constants.Objects.*;
import static com.hazelcast.impl.Constants.Timeouts.DEFAULT_TIMEOUT;
import com.hazelcast.nio.*;
import static com.hazelcast.nio.BufferUtil.toData;
import static com.hazelcast.nio.BufferUtil.toObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecutorManager extends BaseManager implements MembershipListener {

    static ExecutorManager instance = new ExecutorManager();

    private ThreadPoolExecutor executor;

    private final Map<RemoteExecutionId, SimpleExecution> mapRemoteExecutions = new ConcurrentHashMap<RemoteExecutionId, SimpleExecution>(
            1000);

    private final Map<Long, DistributedExecutorAction> mapExecutions = new ConcurrentHashMap<Long, DistributedExecutorAction>(
            100);

    private final BlockingQueue<Long> executionIds = new ArrayBlockingQueue<Long>(100);

    private boolean started = false;

    private ExecutorManager() {
        ClusterService.get().registerPacketProcessor(ClusterOperation.REMOTELY_EXECUTE,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        handleRemoteExecution(packet);
                    }
                });
        ClusterService.get().registerPacketProcessor(ClusterOperation.STREAM, new PacketProcessor() {
            public void process(Packet packet) {
                handleStream(packet);
            }
        });
    }

    public static ExecutorManager get() {
        return instance;
    }

    static class ExecutorThreadFactory implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        ExecutorThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "hz.pool-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            
            return t;
        }
    }

    public void init() {
        super.init();
        if (started) return;
        final int corePoolSize = Config.get().getExecutorConfig().getCorePoolSize();
        final int maxPoolSize = Config.get().getExecutorConfig().getMaxPoolsize();
        final long keepAliveSeconds = Config.get().getExecutorConfig().getKeepAliveSeconds();
        if (DEBUG) {
            log("Executor core:" + corePoolSize + ", max:" + maxPoolSize + ", keepAlive:"
                    + keepAliveSeconds);
        }

        executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveSeconds,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ExecutorThreadFactory());
        Node.get().getClusterImpl().addMembershipListener(this);
        for (int i = 0; i < 100; i++) {
            executionIds.add((long) i);
        }
        started = true;
    }

    public void stop() {
        if (!started) return;
        executionIds.clear();
        executor.shutdownNow();
        started = false;
    }

    public static class CancelationTask implements Callable<Boolean>, DataSerializable {
        long executionId = -1;

        Address address = null;

        boolean mayInterruptIfRunning = false;

        public CancelationTask() {
            super();
        }

        public CancelationTask(final long executionId, final Address address,
                               final boolean mayInterruptIfRunning) {
            super();
            this.executionId = executionId;
            this.address = address;
            this.mayInterruptIfRunning = mayInterruptIfRunning;
        }

        public Boolean call() {
            final SimpleExecution simpleExecution = ExecutorManager.get().mapRemoteExecutions
                    .remove(executionId);
            return simpleExecution != null && simpleExecution.cancel(mayInterruptIfRunning);
        }

        public void readData(final DataInput in) throws IOException {
            executionId = in.readLong();
            address = new Address();
            address.readData(in);
            mayInterruptIfRunning = in.readBoolean();
        }

        public void writeData(final DataOutput out) throws IOException {
            out.writeLong(executionId);
            address.writeData(out);
            out.writeBoolean(mayInterruptIfRunning);
        }

    }

    public class DistributedExecutorAction<T> implements Processable, ExecutionManagerCallback,
            StreamResponseHandler {
        InnerFutureTask<T> innerFutureTask = null;

        DistributedTask<T> distributedFutureTask = null;

        private final int expectedResultCount;

        private final AtomicInteger resultCount = new AtomicInteger();

        protected volatile Data task = null;

        protected volatile Object callable = null;

        protected Address randomTarget = null;

        protected SimpleExecution simpleExecution = null; // for local tasks

        protected Long executionId = null;

        protected final BlockingQueue responseQueue = new LinkedBlockingQueue();

        protected boolean localOnly = true;

        public DistributedExecutorAction(final Long executionId,
                                         final DistributedTask<T> distributedFutureTask, final Data task,
                                         final Object callable) {
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

        public boolean cancel(final boolean mayInterruptIfRunning) {
            if (localOnly) {
                // local
                return simpleExecution.cancel(mayInterruptIfRunning);
            } else {
                // remote
                boolean cancelled = false;
                try {
                    final Callable<Boolean> callCancel = new CancelationTask(executionId
                            , thisAddress, mayInterruptIfRunning);
                    if (innerFutureTask.getMembers() == null) {
                        DistributedTask<Boolean> task;
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
                        cancelled = task.get();
                    } else {
                        // members
                        final MultiTask<Boolean> task = new MultiTask<Boolean>(callCancel,
                                innerFutureTask.getMembers());
                        Hazelcast.getExecutorService().execute(task);
                        final Collection<Boolean> results = task.get();
                        for (final Boolean result : results) {
                            if (result)
                                cancelled = true;
                        }
                    }
                } catch (final Throwable t) {
                    cancelled = true;
                } finally {
                    if (cancelled) {
                        handleStreamResponse(OBJECT_CANCELLED);
                    }
                }
                return cancelled;
            }
        }

        public void executeLocal() {
            simpleExecution = new SimpleExecution(null, executor, this, task, callable, true);
            executor.execute(simpleExecution);
        }

        public Object get() throws InterruptedException {
            return get(-1, null);
        }

        public Object get(final long timeout, final TimeUnit unit) throws InterruptedException {
            try {
                final Object result = (timeout == -1) ? responseQueue.take() : responseQueue.poll(
                        timeout, unit);
                if (result == null)
                    return null;
                if (result instanceof Data) {
                    // returning the remote result
                    // on the client thread
                    return toObject((Data) result);
                } else {
                    // local execution result
                    return result;
                }
            } catch (final InterruptedException e) {
                // if client thread is interrupted
                // finalize for clean interrupt..
                finalizeTask();
                throw e;
            } catch (final Exception e) {
                // shouldn't happen..
                e.printStackTrace();
            }
            return null;
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
                final int resultCountNow = resultCount.incrementAndGet();
                if (resultCountNow >= expectedResultCount) {
                    try {
                        if (response != null)
                            responseQueue.add(response);
                        responseQueue.add(OBJECT_DONE);
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                    finalizeTask();
                } else {
                    responseQueue.add(response);
                }
            }
        }

        public void invoke() {
            enqueueAndReturn(DistributedExecutorAction.this);
        }

        public void process() {
            mapExecutions.put(executionId, this);
            mapStreams.put(executionId, this);
            if (innerFutureTask.getMembers() == null) {
                final Address target = getTarget();
                if (thisAddress.equals(target)) {
                    executeLocal();
                } else {
                    localOnly = false;
                    final Packet packet = obtainPacket("m:exe", null, task,
                            ClusterOperation.REMOTELY_EXECUTE, DEFAULT_TIMEOUT);
                    packet.timeout = DEFAULT_TIMEOUT;
                    packet.longValue = executionId;
                    final boolean sent = send(packet, target);
                    if (!sent) {
                        packet.returnToContainer();
                        handleMemberLeft(getMember(target));
                    }
                }
            } else {
                final Set<Member> members = innerFutureTask.getMembers();
                for (final Member member : members) {
                    if (member.localMember()) {
                        executeLocal();
                    } else {
                        localOnly = false;
                        final Packet packet = obtainPacket("m:exe", null, task,
                                ClusterOperation.REMOTELY_EXECUTE, DEFAULT_TIMEOUT);
                        packet.timeout = DEFAULT_TIMEOUT;
                        packet.longValue = executionId;
                        final boolean sent = send(packet, ((ClusterMember) member).getAddress());
                        if (!sent) {
                            packet.returnToContainer();
                            handleMemberLeft(member);
                        }
                    }
                }
            }

        }

        @Override
        public String toString() {
            return "ExecutorAction [" + executionId + "] expectedResultCount="
                    + expectedResultCount + ", resultCount=" + resultCount;
        }

        protected Address getTarget() {
            Address target = null;
            if (innerFutureTask.getKey() != null) {
                Data keyData = null;
                try {
                    keyData = ThreadContext.get().toData(innerFutureTask.getKey());
                } catch (final Exception e) {
                    e.printStackTrace();
                }
                target = getKeyOwner(keyData);
            } else if (innerFutureTask.getMember() != null) {
                final Object mem = innerFutureTask.getMember();
                if (mem instanceof ClusterMember) {
                    final ClusterMember clusterMember = (ClusterMember) mem;
                    target = clusterMember.getAddress();
                } else if (mem instanceof MemberImpl) {
                    target = ((MemberImpl) mem).getAddress();
                }
                if (DEBUG)
                    log(" Target " + target);
            } else {
                Set<Member> members = Node.get().getClusterImpl().getMembers();
                final int random = (int) (Math.random() * 1000);
                final int randomIndex = random % members.size();
                ClusterMember randomClusterMember = (ClusterMember) members.toArray()[randomIndex];
                target = randomClusterMember.getAddress();
//                target = lsMembers.get(random % lsMembers.size()).getAddress();
                randomTarget = target;
            }
            if (target == null)
                return thisAddress;
            else
                return target;
        }

        void handleMemberLeft(final Member member) {
            boolean found = false;
            final ClusterMember clusterMember = (ClusterMember) member;
            if (innerFutureTask.getKey() != null) {
                final Data keyData = toData(innerFutureTask.getKey());
                final Address target = getKeyOwner(keyData);
                if (clusterMember.getAddress().equals(target)) {
                    found = true;
                }
            } else if (innerFutureTask.getMember() != null) {
                final Member target = innerFutureTask.getMember();
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
                final Set<Member> members = innerFutureTask.getMembers();
                for (final Member targetMember : members) {
                    if (member.equals(targetMember)) {
                        found = true;
                    }
                }
            } else {
                if (clusterMember.getAddress().equals(randomTarget)) {
                    found = true;
                }
            }
            if (!found)
                return;
            innerFutureTask.innerSetMemberLeft(member);
            handleStreamResponse(OBJECT_DONE);

        }

        /**
         * !! Called by multiple threads.
         */
        private void finalizeTask() {
            if (innerFutureTask != null)
                innerFutureTask.innerDone();
            // logger.log(Level.INFO,"finalizing.. " + executionId);
            if (executionId == null)
                return;
            if (innerFutureTask.getExecutionCallback() != null) {
                innerFutureTask.getExecutionCallback().done(distributedFutureTask);
            }
            mapStreams.remove(executionId);
            final Object action = mapExecutions.remove(executionId);
            // logger.log(Level.INFO,"finalizing action  " + action);
            if (action != null) {
                final boolean offered = executionIds.offer(executionId);
                if (!offered)
                    throw new RuntimeException("Couldn't offer the executionId " + executionId);
            }
            executionId = null;
        }
    }

    public class RemoteExecutionId {
        public long executionId;

        public Address address;

        public RemoteExecutionId(final Address address, final long executionId) {
            super();
            this.address = address;
            this.executionId = executionId;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final RemoteExecutionId other = (RemoteExecutionId) obj;
            if (address == null) {
                if (other.address != null)
                    return false;
            } else if (!address.equals(other.address))
                return false;
            return executionId == other.executionId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((address == null) ? 0 : address.hashCode());
            result = prime * result + (int) (executionId ^ (executionId >>> 32));
            return result;
        }
    }

    private static class SimpleExecution implements Runnable {

        protected static Logger logger = Logger.getLogger(SimpleExecution.class.getName());

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

        public SimpleExecution(final RemoteExecutionId remoteExecutionId,
                               final ExecutorService executor, final DistributedExecutorAction action,
                               final Data value, final Object callable, final boolean local) {
            super();
            this.value = value;
            this.callable = callable;
            this.executor = executor;
            this.action = action;
            this.local = local;
            this.remoteExecutionId = remoteExecutionId;
        }

        public static Object call(final RemoteExecutionId remoteExecutionId, final boolean local,
                                  final Object callable) throws InterruptedException {
            Object executionResult = null;
            try {
                if (callable instanceof Callable) {
                    executionResult = ((Callable) callable).call();
                } else if (callable instanceof Runnable) {
                    ((Runnable) callable).run();
                }
            } catch (final InterruptedException e) {
                throw e;
            } catch (final Throwable e) {
                // executionResult = new ExceptionObject(e);
                executionResult = e;
            } finally {
                if (!local) {
                    ExecutorManager.get().mapRemoteExecutions.remove(remoteExecutionId);
                }
            }
            return executionResult;
        }

        public boolean cancel(final boolean mayInterruptIfRunning) {
            if (DEBUG) {
                logger.log(Level.INFO, "SimpleExecution is cancelling..");
            }
            if (done || cancelled)
                return false;
            if (running && mayInterruptIfRunning)
                runningThread.interrupt();
            cancelled = true;
            return true;
        }

        public void run() {
            if (cancelled)
                return;
            Object executionResult = null;
            if (callable == null) {
                callable = toObject(value);
                executor.execute(this);
            } else {
                runningThread = Thread.currentThread();
                running = true;
                try {
                    executionResult = call(remoteExecutionId, local, callable);
                } catch (final InterruptedException e) {
                    cancelled = true;
                }
                if (cancelled)
                    return;
                running = false;
                done = true; // should not be able to interrupt after this.
                if (!local) {
                    try {
                        ExecutorManager.get().sendStreamItem(remoteExecutionId.address,
                                executionResult, remoteExecutionId.executionId);
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    // action.onResponse(executionResult);
                    action.handleStreamResponse(executionResult);
                }
            }
        }
    }

    public void sendStreamItem(final Address address, final Object value, final long streamId) {
        try {
            final Packet packet = ClusterManager.get().obtainPacket("exe", null, value,
                    ClusterOperation.STREAM, DEFAULT_TIMEOUT);
            packet.longValue = streamId;
            ClusterService.get().enqueueAndReturn(new Processable() {
                public void process() {
                    send(packet, address);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Processable createNewExecutionAction(final DistributedTask task) {
        if (task == null)
            throw new RuntimeException("task cannot be null");

        try {
            final Long executionId = executionIds.take();
            final InnerFutureTask inner = (InnerFutureTask) task.getInner();
            final Callable callable = inner.getCallable();
            final Data callableData = ThreadContext.get().toData(callable);
            final DistributedExecutorAction action = new DistributedExecutorAction(executionId,
                    task, callableData, callable);
            inner.setExecutionManagerCallback(action);
            return action;
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void executeLocaly(final Runnable runnable) {
        executor.execute(runnable);
    }

    public void handleStream(final Packet packet) {
        final StreamResponseHandler streamResponseHandler = mapStreams.get(packet.longValue);
        if (streamResponseHandler != null) {
            final Data value = BufferUtil.doTake(packet.value);
            executor.execute(new Runnable() {
                public void run() {
                    streamResponseHandler.handleStreamResponse(value);
                }
            });
        }
        packet.returnToContainer();
    }

    public void handleRemoteExecution(final Packet packet) {
        if (DEBUG)
            log("Remote handling packet " + packet);
        final Data callableData = BufferUtil.doTake(packet.value);
        final RemoteExecutionId remoteExecutionId = new RemoteExecutionId(packet.conn.getEndPoint(),
                packet.longValue);
        final SimpleExecution se = new SimpleExecution(remoteExecutionId, executor, null,
                callableData, null, false);
        mapRemoteExecutions.put(remoteExecutionId, se);
        executor.execute(se);
        packet.returnToContainer();
    }

    public void memberAdded(final MembershipEvent membersipEvent) {
    }

    public void memberRemoved(final MembershipEvent membersipEvent) {
        final Collection<DistributedExecutorAction> executionActions = mapExecutions.values();
        for (final DistributedExecutorAction distributedExecutorAction : executionActions) {
            distributedExecutorAction.handleMemberLeft(membersipEvent.getMember());
        }
    }
}
