/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.ClientDistributedTask;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.ExecutionManagerCallback;
import com.hazelcast.impl.InnerFutureTask;

import java.util.*;
import java.util.concurrent.*;

import static com.hazelcast.nio.IOUtil.toObject;
import static com.hazelcast.client.ProxyHelper.check;

public class ExecutorServiceClientProxy implements ExecutorService {

    final ProxyHelper proxyHelper;
    final ExecutorService callBackExecutors = Executors.newFixedThreadPool(5);

    public ExecutorServiceClientProxy(HazelcastClient client, String name) {
        proxyHelper = new ProxyHelper(name, client);
    }

    public void shutdown() {
    }

    public List<Runnable> shutdownNow() {
        return new ArrayList<Runnable>();
    }

    public boolean isShutdown() {
        throw new UnsupportedOperationException();
    }

    public boolean isTerminated() {
        throw new UnsupportedOperationException();
    }

    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
        return false;
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return submit(new DistributedTask(callable));
    }

    private <T> Future<T> submit(DistributedTask dt) {
        ClientDistributedTask cdt = null;
        InnerFutureTask inner = (InnerFutureTask) dt.getInner();
        check(inner.getCallable());
        if (dt instanceof MultiTask) {
            if (inner.getMembers() == null) {
                Set<Member> set = new HashSet<Member>();
                set.add(inner.getMember());
                cdt = new ClientDistributedTask(inner.getCallable(), null, set, null);
            }
        }
        if (cdt == null) {
            cdt = new ClientDistributedTask(inner.getCallable(), inner.getMember(), inner.getMembers(), inner.getKey());
        }
        return submit(dt, cdt);
    }

//    private <T> void check(Object o) {
//        if (o == null) {
//            throw new NullPointerException("Object cannot be null.");
//        }
//        if (!(o instanceof Serializable)) {
//            throw new IllegalArgumentException(o.getClass().getName() + " is not Serializable.");
//        }
//    }

    private Future submit(final DistributedTask dt, final ClientDistributedTask cdt) {
        final Packet request = proxyHelper.prepareRequest(ClusterOperation.EXECUTE, cdt, null);
        final InnerFutureTask inner = (InnerFutureTask) dt.getInner();
        final Call call = new Call(ProxyHelper.newCallId(), request) {
            public void onDisconnect(final Member member) {
                setResponse(new MemberLeftException(member));
            }

            public void setResponse(Object response) {
                super.setResponse(response);
                if (dt.getExecutionCallback() != null) {
                    callBackExecutors.execute(new Runnable() {
                        public void run() {
                            ((InnerFutureTask) dt.getInner()).innerDone();
                            dt.getExecutionCallback().done(dt);
                        }
                    });
                }
            }
        };
        inner.setExecutionManagerCallback(new ExecutionManagerCallback() {
            private volatile boolean cancelled = false;

            public boolean cancel(boolean mayInterruptIfRunning) {
                cancelled = (Boolean) proxyHelper.doOp(ClusterOperation.CANCEL_EXECUTION, call.getId(), mayInterruptIfRunning);
                return cancelled;
            }

            public void get() throws InterruptedException, ExecutionException {
                if (cancelled) throw new CancellationException();
                try {
                    Object response = call.getResponse();
                    handle(response);
                } catch (Throwable e) {
                    handle(e);
                }
            }

            public void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
                if (cancelled) throw new CancellationException();
                try {
                    Object response = call.getResponse(timeout, unit);
                    handle(response);
                } catch (Throwable e) {
                    handle(e);
                }
            }

            private void handle(Object response) {
                Object result = response;
                if (response == null) {
                    inner.innerSetException(new TimeoutException(), false);
                } else {
                    if (response instanceof Packet) {
                        Packet responsePacket = (Packet) response;
                        result = toObject(responsePacket.getValue());
                    }
                    if (result instanceof MemberLeftException) {
                        MemberLeftException memberLeftException = (MemberLeftException) result;
                        inner.innerSetMemberLeft(memberLeftException.getMember());
                    } else if (result instanceof Throwable) {
                        inner.innerSetException((Throwable) result, true);
                    } else {
                        if (dt instanceof MultiTask) {
                            if (result != null) {
                                Collection colResults = (Collection) result;
                                for (Object obj : colResults) {
                                    inner.innerSet(obj);
                                }
                            } else {
                                inner.innerSet(result);
                            }
                        } else {
                            inner.innerSet(result);
                        }
                    }
                }
                inner.innerDone();
            }
        });
        proxyHelper.sendCall(call);
        return dt;
    }

    public <T> Future<T> submit(Runnable runnable, T t) {
        if (runnable instanceof DistributedTask) {
            return submit((DistributedTask) runnable);
        } else {
            return submit(DistributedTask.callable(runnable, t));
        }
    }

    public Future<?> submit(Runnable runnable) {
        return submit(runnable, null);
    }

    public List<Future> invokeAll(Collection tasks) throws InterruptedException {
        // Inspired to JDK7
        if (tasks == null)
            throw new NullPointerException();
        List<Future> futures = new ArrayList<Future>(tasks.size());
        boolean done = false;
        try {
            for (Object command : tasks) {
                futures.add(submit((Callable) command));
            }
            for (Future f : futures) {
                if (!f.isDone()) {
                    try {
                        f.get();
                    } catch (CancellationException ignore) {
                    } catch (ExecutionException ignore) {
                    }
                }
            }
            done = true;
            return futures;
        } finally {
            if (!done)
                for (Future f : futures)
                    f.cancel(true);
        }
    }

    public List invokeAll(Collection tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public Object invokeAny(Collection tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    public Object invokeAny(Collection tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    public void execute(Runnable runnable) {
        submit(runnable, null);
    }
}
