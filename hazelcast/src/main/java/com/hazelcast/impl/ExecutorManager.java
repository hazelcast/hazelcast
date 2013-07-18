/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.impl;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.Constants.RedoType;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.impl.executor.ParallelExecutorService;
import com.hazelcast.impl.monitor.ExecutorOperationsCounter;
import com.hazelcast.impl.monitor.LocalExecutorOperationStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.Partition;
import com.hazelcast.security.SecureCallable;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.CANCEL_EXECUTION;
import static com.hazelcast.impl.ClusterOperation.EXECUTE;
import static com.hazelcast.impl.Constants.Objects.*;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ExecutorManager extends BaseManager {

    private final ConcurrentMap<String, NamedExecutorService> mapExecutors = new ConcurrentHashMap<String, NamedExecutorService>(10);
    private final ConcurrentMap<Thread, CallContext> mapThreadCallContexts = new ConcurrentHashMap<Thread, CallContext>(100);
    private final ParallelExecutor mapLoaderExecutorService;
    private final ParallelExecutor asyncExecutorService;
    private final NamedExecutorService defaultExecutorService;
    private final NamedExecutorService queryExecutorService;
    private final NamedExecutorService eventExecutorService;

    private volatile boolean started = false;
    private static final String DEFAULT_EXECUTOR_SERVICE = "x:default";
    private static final String QUERY_EXECUTOR_SERVICE = "x:hz.query";
    private static final String STORE_EXECUTOR_SERVICE = "x:hz.store";
    private static final String EVENT_EXECUTOR_SERVICE = "x:hz.events";
    private final Object CREATE_LOCK = new Object();
    private final ParallelExecutorService parallelExecutorService;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final ConcurrentMap<ExecutionKey, RequestExecutor> executions = new ConcurrentHashMap<ExecutionKey, RequestExecutor>(100);
    private final ScheduledThreadPoolExecutor esScheduled;

    private final ConcurrentMap<String, ExecutorOperationsCounter> internalThroughputMap = new ConcurrentHashMap<String, ExecutorOperationsCounter>();
    private final ConcurrentMap<String, ExecutorOperationsCounter> throughputMap = new ConcurrentHashMap<String, ExecutorOperationsCounter>();

    final AtomicLong executionIdGen = new AtomicLong();
    private final int interval = 60000;

    ExecutorManager(final Node node) {
        super(node);
        logger.log(Level.FINEST, "Starting ExecutorManager");
        GroupProperties gp = node.groupProperties;
        ClassLoader classLoader = node.getConfig().getClassLoader();
        ExecutorThreadFactory.ThreadCleanup cleanup = new ExecutorThreadFactory.ThreadCleanup() {
            public void cleanup(Thread t) {
                mapThreadCallContexts.remove(t);
            }
        };
        threadPoolExecutor = new ThreadPoolExecutor(
                5, Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue(),
                new ExecutorThreadFactory(node.threadGroup, node.getThreadPoolNamePrefix("cached"),
                        classLoader, cleanup),
                new RejectionHandler()) {
            protected void beforeExecute(Thread t, Runnable r) {
                threadPoolBeforeExecute(t, r);
            }
        };
        esScheduled = new ScheduledThreadPoolExecutor(3, new ExecutorThreadFactory(node.threadGroup,
                node.getThreadPoolNamePrefix("scheduled"), classLoader, cleanup), new RejectionHandler()) {
            protected void beforeExecute(Thread t, Runnable r) {
                threadPoolBeforeExecute(t, r);
            }
        };
        parallelExecutorService = new ParallelExecutorService(node.getLogger(ParallelExecutorService.class.getName()), threadPoolExecutor);
        defaultExecutorService = getOrCreateNamedExecutorService(DEFAULT_EXECUTOR_SERVICE);
        queryExecutorService = getOrCreateNamedExecutorService(QUERY_EXECUTOR_SERVICE, gp.EXECUTOR_QUERY_THREAD_COUNT);
        eventExecutorService = getOrCreateNamedExecutorService(EVENT_EXECUTOR_SERVICE, gp.EXECUTOR_EVENT_THREAD_COUNT);
        mapLoaderExecutorService = parallelExecutorService.newParallelExecutor(gp.MAP_LOAD_THREAD_COUNT.getInteger());
        asyncExecutorService = parallelExecutorService.newBlockingParallelExecutor(gp.EXECUTOR_QUERY_THREAD_COUNT.getInteger(), 1000);
        newNamedExecutorService(Prefix.EXECUTOR_SERVICE + "hz.initialization", new ExecutorConfig("hz.initialization",
                Integer.MAX_VALUE, Integer.MAX_VALUE, 60));
        registerPacketProcessor(EXECUTE, new ExecutionOperationHandler());
        registerPacketProcessor(CANCEL_EXECUTION, new ExecutionCancelOperationHandler());
        started = true;
    }

    public NamedExecutorService getOrCreateNamedExecutorService(String name) {
        return getOrCreateNamedExecutorService(name, null);
    }

    public ScheduledThreadPoolExecutor getScheduledExecutorService() {
        return esScheduled;
    }

    public ParallelExecutor getMapLoaderExecutorService() {
        return mapLoaderExecutorService;
    }

    private NamedExecutorService getOrCreateNamedExecutorService(String name, GroupProperties.GroupProperty groupProperty) {
        NamedExecutorService namedExecutorService = mapExecutors.get(name);
        if (namedExecutorService == null) {
            synchronized (CREATE_LOCK) {
                namedExecutorService = mapExecutors.get(name);
                if (namedExecutorService == null) {
                    ExecutorConfig executorConfig = node.getConfig().getExecutorConfig(name.substring(2));
                    if (groupProperty != null) {
                        executorConfig.setCorePoolSize(groupProperty.getInteger());
                        executorConfig.setMaxPoolSize(groupProperty.getInteger());
                    }
                    namedExecutorService = newNamedExecutorService(name, executorConfig);
                }
            }
        }
        return namedExecutorService;
    }

    private NamedExecutorService newNamedExecutorService(String name, ExecutorConfig executorConfig) {
        logger.log(Level.FINEST, "creating new named executor service " + name);
        int concurrencyLevel = executorConfig.getMaxPoolSize();
        ParallelExecutor parallelExecutor = parallelExecutorService.newParallelExecutor(concurrencyLevel);
        NamedExecutorService es = new NamedExecutorService(name, parallelExecutor);
        mapExecutors.put(name, es);
        return es;
    }

    public ParallelExecutor newParallelExecutor(int concurrencyLevel) {
        return parallelExecutorService.newParallelExecutor(concurrencyLevel);
    }

    class ExecutionCancelOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            ExecutionKey executionKey = new ExecutionKey(request.caller, request.longValue);
            RequestExecutor requestExecutor = executions.get(executionKey);
            if (requestExecutor != null) {
                request.response = requestExecutor.cancel(request.blockId == 1);
            }
            returnResponse(request);
        }

        public void handle(Request request) {
            doOperation(request);
        }
    }

    class ExecutionOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            if (isCallerKnownMember(request)) {
                NamedExecutorService namedExecutorService = getOrCreateNamedExecutorService(request.name);
                ExecutionKey executionKey = new ExecutionKey(request.caller, request.longValue);
                RequestExecutor requestExecutor = new RequestExecutor(request, executionKey);
                executions.put(executionKey, requestExecutor);
                namedExecutorService.execute(requestExecutor);
            } else {
                returnRedoResponse(request, RedoType.REDO_MEMBER_UNKNOWN);
            }
        }

        public void handle(Request request) {
            doOperation(request);
        }

        boolean isCallerKnownMember(Request request) {
            return (request.local || getMember(request.caller) != null);
        }
    }

    class ExecutionKey {
        final Address from;
        final long executionId;

        ExecutionKey(Address from, long executionId) {
            this.executionId = executionId;
            this.from = from;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExecutionKey that = (ExecutionKey) o;
            if (executionId != that.executionId) return false;
            return from.equals(that.from);
        }

        @Override
        public int hashCode() {
            int result = from.hashCode();
            result = 31 * result + (int) (executionId ^ (executionId >>> 32));
            return result;
        }
    }

    class RequestExecutor implements Runnable {
        final Request request;
        private final ExecutionKey executionKey;
        volatile boolean done = false;
        volatile boolean cancelled = false;
        volatile boolean running = false;
        volatile Thread runningThread = null;
        final long creationTime;

        RequestExecutor(Request request, ExecutionKey executionKey) {
            this.request = request;
            this.executionKey = executionKey;
            this.creationTime = Clock.currentTimeMillis();
            internalThroughputMap.putIfAbsent(request.name, new ExecutorOperationsCounter(interval, request.name));
            internalThroughputMap.get(request.name).startPending();
        }

        public void run() {
            final long startTime = Clock.currentTimeMillis();
            Object result = null;
            final ExecutorOperationsCounter operationsCounter = internalThroughputMap.get(request.name);
            try {
                runningThread = Thread.currentThread();
                operationsCounter.startExecution(startTime - creationTime);
                running = true;
                if (!cancelled) {
                    Callable callable = (Callable) toObject(request.value);
                    if (callable instanceof SecureCallable) {
                        final SecureCallable secureCallable = (SecureCallable) callable;
                        secureCallable.setNode(node);
                    }
                    result = callable.call();
                    result = toData(result);
                }
            } catch (Throwable e) {
                result = toData(e);
            } finally {
                if (cancelled) {
                    result = toData(new CancellationException());
                }
                operationsCounter.finishExecution(Clock.currentTimeMillis() - startTime);
                running = false;
                done = true;
                executions.remove(executionKey);
                request.clearForResponse();
                request.response = result;
                enqueueAndReturn(new ReturnResponseProcess(request));
            }
        }

        public boolean cancel(final boolean mayInterruptIfRunning) {
            if (done || cancelled) {
                return false;
            }
            cancelled = true;
            if (running && mayInterruptIfRunning && runningThread != null) {
                runningThread.interrupt();
            }
            return true;
        }
    }

    public void appendState(StringBuffer sbState) {
        Set<String> names = mapExecutors.keySet();
        for (String name : names) {
            NamedExecutorService namedExecutorService = mapExecutors.get(name);
            namedExecutorService.appendState(sbState);
        }
    }

    public Set<String> getExecutorNames() {
        return mapExecutors.keySet();
    }

    public void appendFullState(StringBuffer sbState) {
        Set<String> names = mapExecutors.keySet();
        for (String name : names) {
            NamedExecutorService namedExecutorService = mapExecutors.get(name);
            namedExecutorService.appendState(sbState);
        }
    }

    class RejectionHandler implements RejectedExecutionHandler {
        public void rejectedExecution(Runnable runnable, ThreadPoolExecutor threadPoolExecutor) {
            //ignored
            logger.log(Level.WARNING, "ExecutorService is rejecting an execution. " + runnable);
        }
    }

    public void stop() {
        if (!started) return;
        Collection<NamedExecutorService> executors = mapExecutors.values();
        for (NamedExecutorService namedExecutorService : executors) {
            namedExecutorService.stop();
        }
        parallelExecutorService.shutdown();
        esScheduled.shutdownNow();
        threadPoolExecutor.shutdownNow();
        try {
            esScheduled.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        try {
            threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        started = false;
    }

    public NamedExecutorService getDefaultExecutorService() {
        return defaultExecutorService;
    }

    public NamedExecutorService getEventExecutorService() {
        return eventExecutorService;
    }

    public void executeLocally(Runnable runnable) {
        defaultExecutorService.execute(runnable);
    }

    public void executeAsync(Runnable runnable) {
        asyncExecutorService.execute(runnable);
    }

    public void executeNow(Runnable runnable) {
        threadPoolExecutor.execute(runnable);
    }

    public void executeQueryTask(Runnable runnable) {
        queryExecutorService.execute(runnable);
    }

    public void call(String name, DistributedTask dtask) {
        NamedExecutorService namedExecutorService = getOrCreateNamedExecutorService(name);
        InnerFutureTask inner = (InnerFutureTask) dtask.getInner();
        Data dataCallable = toData(inner.getCallable());
        if (inner.getMembers() != null) {
            Set<Member> members = inner.getMembers();
            if (members.size() == 1) {
                MemberCall memberCall = new MemberCall(name, (MemberImpl) members.iterator().next(), dataCallable, dtask);
                inner.setExecutionManagerCallback(memberCall);
                memberCall.call();
            } else {
                MembersCall membersCall = new MembersCall(name, members, dataCallable, dtask);
                inner.setExecutionManagerCallback(membersCall);
                membersCall.call();
            }
        } else if (inner.getMember() != null) {
            MemberCall memberCall = new MemberCall(name, (MemberImpl) inner.getMember(), dataCallable, dtask);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        } else if (inner.getKey() != null) {
            Partition partition = node.factory.getPartitionService().getPartition(inner.getKey());
            Member target = partition.getOwner();
            if (target == null) {
                target = node.factory.getCluster().getMembers().iterator().next();
            }
            MemberCall memberCall = new MemberCall(name, (MemberImpl) target, dataCallable, dtask);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        } else {
            MemberImpl target = (MemberImpl) namedExecutorService.getExecutionLoadBalancer().getTarget(node.factory);
            MemberCall memberCall = new MemberCall(name, target, dataCallable, dtask);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        }
    }

    void notifyCompletion(final DistributedTask dtask) {
        final InnerFutureTask innerFutureTask = (InnerFutureTask) dtask.getInner();
        getEventExecutorService().execute(new Runnable() {
            public void run() {
                innerFutureTask.innerDone();
                if (innerFutureTask.getExecutionCallback() != null) {
                    innerFutureTask.getExecutionCallback().done(dtask);
                }
            }
        });
    }

    private void threadPoolBeforeExecute(Thread t, Runnable r) {
        ThreadContext threadContext = ThreadContext.get();
        threadContext.setCurrentFactory(node.factory);
        CallContext callContext = mapThreadCallContexts.get(t);
        if (callContext == null) {
            callContext = new CallContext(threadContext.createNewThreadId(), false);
            mapThreadCallContexts.put(t, callContext);
        }
        threadContext.setCallContext(callContext);
    }

    class MembersCall implements ExecutionManagerCallback, ExecutionListener {
        final DistributedTask dtask;
        final String name;
        final Set<Member> members;
        final Data callable;
        final InnerFutureTask innerFutureTask;
        final List<MemberCall> lsMemberCalls = new ArrayList<MemberCall>();
        int responseCount = 0;
        long startTime;

        MembersCall(String name, Set<Member> members, Data callable, DistributedTask dtask) {
            this.name = name;
            this.members = members;
            this.callable = callable;
            this.dtask = dtask;
            this.innerFutureTask = (InnerFutureTask) dtask.getInner();
        }

        void call() {
            throughputMap.putIfAbsent(name, new ExecutorOperationsCounter(interval, name));
            throughputMap.get(name).startExecution(0);
            startTime = Clock.currentTimeMillis();
            for (Member member : members) {
                MemberCall memberCall = new MemberCall(name, (MemberImpl) member, callable, dtask, false, this);
                lsMemberCalls.add(memberCall);
                memberCall.call();
            }
        }

        public void onResponse(Object result) {
            throughputMap.get(name).finishExecution(Clock.currentTimeMillis() - startTime);
            responseCount++;
            if (result == OBJECT_MEMBER_LEFT || responseCount >= lsMemberCalls.size()) {
                notifyCompletion(dtask);
            }
        }

        public boolean cancel(final boolean mayInterruptIfRunning) {
            throughputMap.get(name).finishExecution(Clock.currentTimeMillis() - startTime);
            List<AsyncCall> lsCancellationCalls = new ArrayList<AsyncCall>(lsMemberCalls.size());
            for (final MemberCall memberCall : lsMemberCalls) {
                AsyncCall asyncCall = new AsyncCall() {
                    @Override
                    protected void call() {
                        this.setResult(memberCall.cancel(mayInterruptIfRunning));
                    }
                };
                lsCancellationCalls.add(asyncCall);
                executeAsync(asyncCall);
            }
            for (AsyncCall cancellationCall : lsCancellationCalls) {
                try {
                    if (cancellationCall.get(5, TimeUnit.SECONDS) == Boolean.TRUE) {
                        return true;
                    }
                } catch (Exception ignored) {
                    return false;
                }
            }
            return false;
        }

        public void get() throws InterruptedException, ExecutionException {
            doGet(-1);
        }

        public void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
            doGet(unit.toMillis(timeout));
        }

        void doGet(long timeoutMillis) {
            boolean done = true;
            long remainingMillis = timeoutMillis;
            try {
                for (MemberCall memberCall : lsMemberCalls) {
                    long now = Clock.currentTimeMillis();
                    if (timeoutMillis == -1) {
                        memberCall.get();
                    } else {
                        if (remainingMillis < 0) {
                            done = false;
                            innerFutureTask.innerSetException(new TimeoutException(), done);
                            return;
                        }
                        memberCall.get(remainingMillis, TimeUnit.MILLISECONDS);
                    }
                    remainingMillis -= (Clock.currentTimeMillis() - now);
                }
            } catch (Exception e) {
                innerFutureTask.innerSetException(e, done);
            } finally {
                if (done) {
                    innerFutureTask.innerDone();
                }
            }
        }
    }

    interface ExecutionListener {
        void onResponse(Object result);
    }

    class TaskCancellationCall extends TargetAwareOp {
        final String name;
        final MemberImpl member;
        final long executionId;
        final boolean mayInterruptIfRunning;

        TaskCancellationCall(String name, MemberImpl member, long executionId, boolean mayInterruptIfRunning) {
            this.name = name;
            this.member = member;
            this.executionId = executionId;
            this.mayInterruptIfRunning = mayInterruptIfRunning;
        }

        public boolean cancel() {
            request.setLocal(CANCEL_EXECUTION, name, null, null, -1, 0L, -1L, thisAddress);
            request.longValue = executionId;
            request.blockId = (mayInterruptIfRunning) ? 1 : 0;
            doOp();
            return getResultAsBoolean();
        }

        @Override
        public void setTarget() {
            target = member.getAddress();
        }

        @Override
        protected boolean canTimeout() {
            return false;
        }
    }

    class MemberCall extends TargetAwareOp implements ExecutionManagerCallback {
        final String name;
        final MemberImpl member;
        final Data callable;
        final DistributedTask dtask;
        final InnerFutureTask innerFutureTask;
        final boolean singleTask;
        final ExecutionListener executionListener;
        @SuppressWarnings("VolatileLongOrDoubleField")
        volatile long executionId;
        long startTime;

        MemberCall(String name, MemberImpl member, Data callable, DistributedTask dtask) {
            this(name, member, callable, dtask, true, null);
        }

        MemberCall(String name, MemberImpl member, Data callable, DistributedTask dtask, boolean singleTask, ExecutionListener executionListener) {
            this.name = name;
            this.member = member;
            this.callable = callable;
            this.dtask = dtask;
            this.innerFutureTask = (InnerFutureTask) dtask.getInner();
            this.singleTask = singleTask;
            this.target = member.getAddress();
            this.executionListener = executionListener;
        }

        public void call() {
            throughputMap.putIfAbsent(name, new ExecutorOperationsCounter(interval, name));
            throughputMap.get(name).startExecution(0);
            startTime = Clock.currentTimeMillis();
            executionId = executionIdGen.incrementAndGet();
            request.setLocal(EXECUTE, name, null, callable, -1, -1, -1, thisAddress);
            request.longValue = executionId;
            // wait max. 10 seconds for ensuring the connection
            // before the call
            if (!member.localMember()) {
                for (int i = 0; i < 10 && node.isActive(); i++) {
                    if (!node.getClusterImpl().getMembers().contains(member)) {
                        break;
                    }
                    if (node.connectionManager.getOrConnect(member.getAddress()) != null) {
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            doOp();
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throughputMap.get(name).finishExecution(Clock.currentTimeMillis() - startTime);
            TaskCancellationCall call = new TaskCancellationCall(name, member, executionId, mayInterruptIfRunning);
            return call.cancel();
        }

        public void get() throws InterruptedException {
            get(-1, null);
        }

        public void get(long time, TimeUnit unit) throws InterruptedException {
            Object result = null;
            boolean done = true;
            try {
                result = doGetResult((time == -1) ? -1 : unit.toMillis(time));
                if (result == OBJECT_NO_RESPONSE || result == OBJECT_REDO) {
                    done = false;
                    innerFutureTask.innerSetException(new TimeoutException(), false);
                } else if (result instanceof CancellationException) {
                    innerFutureTask.innerSetCancelled();
                } else if (result == OBJECT_MEMBER_LEFT) {
                    innerFutureTask.innerSetMemberLeft(member);
                } else if (result instanceof Throwable) {
                    innerFutureTask.innerSetException((Throwable) result, true);
                } else {
                    innerFutureTask.innerSet(result);
                }
            } catch (Exception e) {
                if (time > 0 && e instanceof OperationTimeoutException) {
                    e = new TimeoutException();
                }
                innerFutureTask.innerSetException(e, done);
            } finally {
                if (singleTask && done) {
                    innerFutureTask.innerDone();
                }
            }
        }

        public Object doGetResult(long timeoutMillis) throws InterruptedException {
            Object result = (timeoutMillis == -1) ? getResult() : getResult(timeoutMillis, TimeUnit.MILLISECONDS);
            if (result == null) {
                result = OBJECT_NO_RESPONSE;
            }
            if (result == OBJECT_NULL) {
                result = null;
            } else {
                if (result instanceof Data) {
                    final Data data = (Data) result;
                    if (data.size() == 0) {
                        result = null;
                    } else {
                        result = toObjectWithConfigClassLoader(data);
                    }
                }
            }
            afterGettingResult(request);
            return result;
        }

        @Override
        public void onDisconnect(final Address dead) {
            if (dead.equals(target)) {
                setResult(OBJECT_MEMBER_LEFT);
            }
        }

        @Override
        public void packetNotSent() {
            setResult(OBJECT_MEMBER_LEFT);
        }

        @Override
        protected void memberDoesNotExist() {
            setResult(OBJECT_MEMBER_LEFT);
        }

        public void onResponse(Object response) {
            throughputMap.get(name).finishExecution(Clock.currentTimeMillis() - startTime);
            if (singleTask) {
                notifyCompletion(dtask);
            }
        }

        @Override
        public void setResult(Object result) {
            super.setResult(result);
            if (executionListener != null) {
                executionListener.onResponse(result);
            }
            onResponse(result);
        }

        @Override
        public void setTarget() {
            target = member.getAddress();
        }

        @Override
        protected boolean canTimeout() {
            return false;
        }
    }

    Object toObjectWithConfigClassLoader(Data data) {
        ClassLoader actualContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(node.getConfig().getClassLoader());
            return toObject(data);
        } finally {
            Thread.currentThread().setContextClassLoader(actualContextClassLoader);
        }
    }

    public Map<String, LocalExecutorOperationStatsImpl> getThroughputMap() {
        Map<String, LocalExecutorOperationStatsImpl> map = new ConcurrentHashMap<String, LocalExecutorOperationStatsImpl>();
        for (String s : throughputMap.keySet()) {
            map.put(s, (LocalExecutorOperationStatsImpl) throughputMap.get(s).getPublishedStats());
        }
        return map;
    }

    public Map<String, LocalExecutorOperationStatsImpl> getInternalThroughputMap() {
        Map<String, LocalExecutorOperationStatsImpl> map = new ConcurrentHashMap<String, LocalExecutorOperationStatsImpl>();
        for (String s : internalThroughputMap.keySet()) {
            map.put(s, (LocalExecutorOperationStatsImpl) internalThroughputMap.get(s).getPublishedStats());
        }
        return map;
    }
}
