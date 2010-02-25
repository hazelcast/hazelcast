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

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.Partition;

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
    private final NamedExecutorService migrationExecutorService;
    private final NamedExecutorService queryExecutorService;
    private final NamedExecutorService storeExecutorService;
    private final NamedExecutorService eventExecutorService;
    private volatile boolean started = false;

    private static final String MIGRATION_EXECUTOR_SERVICE = "x:hz.migration";
    private static final String QUERY_EXECUTOR_SERVICE = "x:hz.query";
    private static final String STORE_EXECUTOR_SERVICE = "x:hz.store";
    private static final String EVENT_EXECUTOR_SERVICE = "x:hz.events";

    ExecutorManager(final Node node) {
        super(node);
        logger.log(Level.FINEST, "Starting ExecutorManager");
        Config config = node.getConfig();
        ClassLoader classLoader = config.getClassLoader();
        defaultExecutorService = newNamedExecutorService(classLoader, config.getExecutorConfig());
        migrationExecutorService = newNamedExecutorService(classLoader, new ExecutorConfig(MIGRATION_EXECUTOR_SERVICE, 10, 10, 600));
        queryExecutorService = newNamedExecutorService(classLoader, new ExecutorConfig(QUERY_EXECUTOR_SERVICE, 10, 10, 600));
        storeExecutorService = newNamedExecutorService(classLoader, new ExecutorConfig(STORE_EXECUTOR_SERVICE, 10, 10, 600));
        eventExecutorService = newNamedExecutorService(classLoader, new ExecutorConfig(EVENT_EXECUTOR_SERVICE, 10, 10, 600));
        registerPacketProcessor(EXECUTE, new ExecutionOperationHandler());
        started = true;
    }

    NamedExecutorService newNamedExecutorService(ClassLoader classLoader, ExecutorConfig executorConfig) {
        ThreadPoolExecutor threadPoolExecutor
                = new ThreadPoolExecutor(
                executorConfig.getCorePoolSize(),
                executorConfig.getMaxPoolSize(),
                executorConfig.getKeepAliveSeconds(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ExecutorThreadFactory(node.threadGroup, node.getName() + "." + executorConfig.getName(), classLoader),
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
        String name = executorConfig.getName();
        NamedExecutorService es = new NamedExecutorService(classLoader, executorConfig, threadPoolExecutor);
        mapExecutors.put(name, es);
        return es;
    }

    class ExecutionOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            NamedExecutorService namedExecutorService = mapExecutors.get(request.name);
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
                result = e;
            } finally {
                request.clearForResponse();
                request.response = result;
                enqueueAndReturn(new Processable() {
                    public void process() {
                        returnResponse(request);
                    }
                });
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
        Collection<NamedExecutorService> executors = mapExecutors.values();
        for (NamedExecutorService namedExecutorService : executors) {
            namedExecutorService.stop();
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
        NamedExecutorService namedExecutorService = mapExecutors.get(name);
        InnerFutureTask inner = (InnerFutureTask) dtask.getInner();
        Data callable = toData(inner.getCallable());
        if (inner.getMembers() != null) {
            Set<Member> members = inner.getMembers();
            if (members.size() == 1) {
                MemberCall memberCall = new MemberCall(name, (MemberImpl) members.iterator().next(), callable, inner, true);
                inner.setExecutionManagerCallback(memberCall);
                memberCall.call();
            } else {
                MembersCall membersCall = new MembersCall(name, members, callable, inner);
                inner.setExecutionManagerCallback(membersCall);
                membersCall.call();
            }
        } else if (inner.getMember() != null) {
            MemberCall memberCall = new MemberCall(name, (MemberImpl) inner.getMember(), callable, inner, true);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        } else if (inner.getKey() != null) {
            Partition partition = node.factory.getPartitionService().getPartition(inner.getKey());
            Member target = partition.getOwner();
            if (target == null) {
                target = node.factory.getCluster().getMembers().iterator().next();
            }
            MemberCall memberCall = new MemberCall(name, (MemberImpl) target, callable, inner, true);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        } else {
            MemberImpl target = (MemberImpl) namedExecutorService.getExecutionLoadBalancer().getTarget(node.factory);
            MemberCall memberCall = new MemberCall(name, target, callable, inner, true);
            inner.setExecutionManagerCallback(memberCall);
            memberCall.call();
        }
    }

    class MembersCall implements ExecutionManagerCallback {
        final String name;
        final Set<Member> members;
        final Data callable;
        final InnerFutureTask innerFutureTask;
        final List<MemberCall> lsMemberCalls = new ArrayList<MemberCall>();

        MembersCall(String name, Set<Member> members, Data callable, InnerFutureTask innerFutureTask) {
            this.name = name;
            this.members = members;
            this.callable = callable;
            this.innerFutureTask = innerFutureTask;
        }

        void call() {
            for (Member member : members) {
                MemberCall memberCall = new MemberCall(name, (MemberImpl) member, callable, innerFutureTask, false);
                lsMemberCalls.add(memberCall);
                memberCall.call();
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
                    if (innerFutureTask.isDone()) {
                        return;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                innerFutureTask.innerSetException(e);
            } finally {
                innerFutureTask.innerDone();
            }
        }
    }

    class MemberCall extends TargetAwareOp implements ExecutionManagerCallback {
        final String name;
        final MemberImpl member;
        final Data callable;
        final InnerFutureTask innerFutureTask;
        final boolean singleTask;

        MemberCall(String name, MemberImpl member, Data callable, InnerFutureTask innerFutureTask, boolean singleTask) {
            this.name = name;
            this.member = member;
            this.callable = callable;
            this.innerFutureTask = innerFutureTask;
            this.singleTask = singleTask;
            this.target = member.getAddress();
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