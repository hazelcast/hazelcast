/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.impl.executor.ParallelExecutorService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.Partition;
import com.hazelcast.util.SimpleBlockingQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.EXECUTE;
import static com.hazelcast.impl.Constants.Objects.OBJECT_MEMBER_LEFT;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ExecutorManager extends BaseManager {

    private final ConcurrentMap<String, NamedExecutorService> mapExecutors = new ConcurrentHashMap<String, NamedExecutorService>(10);

    private final ConcurrentMap<Thread, CallContext> mapThreadCallContexts = new ConcurrentHashMap<Thread, CallContext>(100);

    private final NamedExecutorService defaultExecutorService;
    private final NamedExecutorService clientExecutorService;
    private final NamedExecutorService migrationExecutorService;
    private final NamedExecutorService queryExecutorService;
    private final NamedExecutorService storeExecutorService;
    private final NamedExecutorService eventExecutorService;
    private volatile boolean started = false;

    private static final String DEFAULT_EXECUTOR_SERVICE = "x:default";
    private static final String CLIENT_EXECUTOR_SERVICE = "x:hz.client";
    private static final String MIGRATION_EXECUTOR_SERVICE = "x:hz.migration";
    private static final String QUERY_EXECUTOR_SERVICE = "x:hz.query";
    private static final String STORE_EXECUTOR_SERVICE = "x:hz.store";
    private static final String EVENT_EXECUTOR_SERVICE = "x:hz.events";
    private final Object CREATE_LOCK = new Object();
    private final ParallelExecutorService parallelExecutorService;
    private final ThreadPoolExecutor threadPoolExecutor;

    ExecutorManager(final Node node) {
        super(node);
        logger.log(Level.FINEST, "Starting ExecutorManager");
        GroupProperties gp = node.groupProperties;
        ClassLoader classLoader = node.getConfig().getClassLoader();
        threadPoolExecutor = new ThreadPoolExecutor(
                0, Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue(),
                new ExecutorThreadFactory(node.threadGroup, getThreadNamePrefix("cached"), classLoader),
                new RejectionHandler()) {
            protected void beforeExecute(Thread t, Runnable r) {
                ThreadContext threadContext = ThreadContext.get();
                CallContext callContext = mapThreadCallContexts.get(t);
                if (callContext == null) {
                    callContext = new CallContext(threadContext.createNewThreadId(), false);
                    mapThreadCallContexts.put(t, callContext);
                }
                threadContext.setCurrentFactory(node.factory);
                threadContext.setCallContext(callContext);
            }
        };
        parallelExecutorService = new ParallelExecutorService(threadPoolExecutor);
        defaultExecutorService = getOrCreateNamedExecutorService(DEFAULT_EXECUTOR_SERVICE);
        clientExecutorService = getOrCreateNamedExecutorService(CLIENT_EXECUTOR_SERVICE, gp.EXECUTOR_CLIENT_THREAD_COUNT);
        migrationExecutorService = getOrCreateNamedExecutorService(MIGRATION_EXECUTOR_SERVICE, gp.EXECUTOR_MIGRATION_THREAD_COUNT);
        queryExecutorService = getOrCreateNamedExecutorService(QUERY_EXECUTOR_SERVICE, gp.EXECUTOR_QUERY_THREAD_COUNT);
        storeExecutorService = getOrCreateNamedExecutorService(STORE_EXECUTOR_SERVICE, gp.EXECUTOR_STORE_THREAD_COUNT);
        eventExecutorService = getOrCreateNamedExecutorService(EVENT_EXECUTOR_SERVICE, gp.EXECUTOR_EVENT_THREAD_COUNT);
        registerPacketProcessor(EXECUTE, new ExecutionOperationHandler());
        started = true;
    }

    public NamedExecutorService getOrCreateNamedExecutorService(String name) {
        return getOrCreateNamedExecutorService(name, null);
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

    public String getThreadNamePrefix(String executorServiceName) {
        return "hz.executor." + node.getName() + "." + executorServiceName + ".thread-";
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

    class ExecutionOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            NamedExecutorService namedExecutorService = getOrCreateNamedExecutorService(request.name);
            namedExecutorService.execute(new RequestExecutor(request));
        }

        public void handle(Request request) {
            doOperation(request);
        }
    }

    class RequestExecutor implements Runnable {
        final Request request;

        RequestExecutor(Request request) {
            this.request = request;
        }

        public void run() {
            Object result = null;
            try {
                Callable callable = (Callable) toObject(request.value);
                result = callable.call();
                result = toData(result);
            } catch (Throwable e) {
                result = toData(e);
            } finally {
                request.clearForResponse();
                request.response = result;
                enqueueAndReturn(new ReturnResponseProcess(request));
            }
        }
    }

    /**
     * Return true if the ExecutorManager is started and can accept task.
     *
     * @return the ExecutorManager running status
     */
    public boolean isStarted() {
        return started;
    }

    public void appendState(StringBuffer sbState) {
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
        parallelExecutorService.shutdown();
        Collection<NamedExecutorService> executors = mapExecutors.values();
        for (NamedExecutorService namedExecutorService : executors) {
            namedExecutorService.stop();
        }
        started = false;
    }

    public NamedExecutorService getDefaultExecutorService() {
        return defaultExecutorService;
    }

    public NamedExecutorService getClientExecutorService() {
        return clientExecutorService;
    }

    public NamedExecutorService getEventExecutorService() {
        return eventExecutorService;
    }

    public void executeLocally(Runnable runnable) {
        defaultExecutorService.execute(runnable);
    }

    public void executeNow(Runnable runnable) {
       threadPoolExecutor.execute(runnable);
    }

    public void executeMigrationTask(Runnable runnable) {
        migrationExecutorService.execute(runnable);
    }

    public void executeQueryTask(Runnable runnable) {
        queryExecutorService.execute(runnable);
    }

    public void executeStoreTask(Runnable runnable) {
        storeExecutorService.execute(runnable);
    }

    public void call(String name, DistributedTask dtask) {
        NamedExecutorService namedExecutorService = getOrCreateNamedExecutorService(name);
        InnerFutureTask inner = (InnerFutureTask) dtask.getInner();
        Data callable = toData(inner.getCallable());
        if (inner.getMembers() != null) {
            Set<Member> members = inner.getMembers();
            if (members.size() == 1) {
                MemberCall memberCall = new MemberCall(name, (MemberImpl) members.iterator().next(), callable, dtask);
                inner.setExecutionManagerCallback(memberCall);
                memberCall.call();
            } else {
                MembersCall membersCall = new MembersCall(name, members, callable, dtask);
                inner.setExecutionManagerCallback(membersCall);
                membersCall.call();
            }
        } else if (inner.getMember() != null) {
            MemberCall memberCall = new MemberCall(name, (MemberImpl) inner.getMember(), callable, dtask);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        } else if (inner.getKey() != null) {
            Partition partition = node.factory.getPartitionService().getPartition(inner.getKey());
            Member target = partition.getOwner();
            if (target == null) {
                target = node.factory.getCluster().getMembers().iterator().next();
            }
            MemberCall memberCall = new MemberCall(name, (MemberImpl) target, callable, dtask);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        } else {
            MemberImpl target = (MemberImpl) namedExecutorService.getExecutionLoadBalancer().getTarget(node.factory);
            MemberCall memberCall = new MemberCall(name, target, callable, dtask);
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

    class MembersCall implements ExecutionManagerCallback, ExecutionListener {
        final DistributedTask dtask;
        final String name;
        final Set<Member> members;
        final Data callable;
        final InnerFutureTask innerFutureTask;
        final List<MemberCall> lsMemberCalls = new ArrayList<MemberCall>();
        int responseCount = 0;

        MembersCall(String name, Set<Member> members, Data callable, DistributedTask dtask) {
            this.name = name;
            this.members = members;
            this.callable = callable;
            this.dtask = dtask;
            this.innerFutureTask = (InnerFutureTask) dtask.getInner();
        }

        void call() {
            for (Member member : members) {
                MemberCall memberCall = new MemberCall(name, (MemberImpl) member, callable, dtask, false, this);
                lsMemberCalls.add(memberCall);
                memberCall.call();
            }
        }

        public void onResponse(Object result) {
            responseCount++;
            if (result == OBJECT_MEMBER_LEFT || responseCount >= lsMemberCalls.size()) {
                notifyCompletion(dtask);
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        public void get() throws InterruptedException, ExecutionException {
            doGet(-1);
        }

        public void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
            doGet(unit.toMillis(timeout));
        }

        void doGet(long timeoutMillis) {
            long remainingMillis = timeoutMillis;
            try {
                for (MemberCall memberCall : lsMemberCalls) {
                    long now = System.currentTimeMillis();
                    if (timeoutMillis == -1) {
                        memberCall.get();
                    } else {
                        if (remainingMillis < 0) {
                            innerFutureTask.innerSetException(new TimeoutException());
                            return;
                        }
                        memberCall.get(remainingMillis, TimeUnit.MILLISECONDS);
                    }
                    remainingMillis -= (System.currentTimeMillis() - now);
                }
            } catch (Exception e) {
                e.printStackTrace();
                innerFutureTask.innerSetException(e);
            } finally {
                innerFutureTask.innerDone();
            }
        }
    }

    interface ExecutionListener {
        void onResponse(Object result);
    }

    class MemberCall extends TargetAwareOp implements ExecutionManagerCallback {
        final String name;
        final MemberImpl member;
        final Data callable;
        final DistributedTask dtask;
        final InnerFutureTask innerFutureTask;
        final boolean singleTask;
        final ExecutionListener executionListener;

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
            request.setLocal(EXECUTE, name, null, callable, -1, -1, -1, thisAddress);
            doOp();
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        public void get() throws InterruptedException {
            get(-1, null);
        }

        public void get(long time, TimeUnit unit) throws InterruptedException {
            try {
                Object result = doGetResult((time == -1) ? -1 : unit.toMillis(time));
                if (result == OBJECT_MEMBER_LEFT) {
                    innerFutureTask.innerSetMemberLeft(member);
                } else if (result instanceof Throwable) {
                    innerFutureTask.innerSetException((Throwable) result);
                } else {
                    innerFutureTask.innerSet(result);
                }
            } catch (Exception e) {
                innerFutureTask.innerSetException(e);
            } finally {
                if (singleTask) {
                    innerFutureTask.innerDone();
                }
            }
        }

        public Object doGetResult(long timeoutMillis) throws InterruptedException {
            Object result = (timeoutMillis == -1) ? getResult() : getResult(timeoutMillis, TimeUnit.MILLISECONDS);
            if (result == null) {
                result = new TimeoutException();
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

        public void onResponse(Object response) {
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
            this.target = member.getAddress();
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
}