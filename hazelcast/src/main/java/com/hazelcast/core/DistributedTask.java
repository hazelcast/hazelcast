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

package com.hazelcast.core;

import com.hazelcast.impl.DistributedRunnableAdapter;
import com.hazelcast.impl.ExecutionManagerCallback;
import com.hazelcast.impl.InnerFutureTask;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A cancellable asynchronous distributed computation.
 * <p/>
 * This class is analogue to java.util.concurrent.FutureTask.
 * <p/>
 * Once the computation has completed, the computation cannot
 * be restarted or cancelled.
 */
public class DistributedTask<V> extends FutureTask<V> {
    private volatile V result = null;

    private volatile Throwable exception = null;

    private final Inner inner;

    private volatile boolean done = false;

    private volatile boolean passed = false;

    private volatile boolean cancelled = false;

    private volatile MemberLeftException memberLeftException = null;

    private DistributedTask(Callable<V> callable, Member member, Set<Member> members, Object key) {
        super(callable);
        if (key == null && member == null && members == null) {
            if (callable instanceof DistributedRunnableAdapter) {
                DistributedRunnableAdapter<V> dra = (DistributedRunnableAdapter) callable;
                Runnable runnable = dra.getRunnable();
                if (runnable instanceof PartitionAware) {
                    key = ((PartitionAware) runnable).getPartitionKey();
                }
                this.result = dra.getResult();
            } else if (callable instanceof PartitionAware) {
                key = ((PartitionAware) callable).getPartitionKey();
            }
        }
        if (key != null) {
            check(key);
        }
        if (members != null) {
            if (members instanceof ISet) {
                Set<Member> newMembers = new HashSet<Member>();
                for (Member mem : members) {
                    newMembers.add(mem);
                }
                members = newMembers;
            }
            if (members.size() == 1) {
                for (Member m : members) {
                    member = m;
                }
                members = null;
            }
        }
        inner = new Inner(callable, member, members, key);
    }

    private void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Cannot be null.");
        }
        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    public DistributedTask(Callable<V> callable) {
        this(callable, null, null, null);
    }

    public DistributedTask(Callable<V> callable, Member member) {
        this(callable, member, null, null);
    }

    protected DistributedTask(Callable<V> callable, Set<Member> members) {
        this(callable, null, members, null);
    }

    public DistributedTask(Callable<V> callable, Object key) {
        this(callable, null, null, key);
    }

    public DistributedTask(Runnable task, V result) {
        this(callable(task, result));
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        if (!done || !passed) {
            inner.get();
            passed = true;
        }
        if (cancelled) {
            throw new CancellationException();
        } else if (memberLeftException != null) {
            throw memberLeftException;
        } else if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!done || !passed) {
            inner.get(timeout, unit);
            passed = true;
        }
        if (cancelled)
            throw new CancellationException();
        if (memberLeftException != null)
            throw memberLeftException;
        if (exception != null) {
            if (exception instanceof TimeoutException) {
                throw (TimeoutException) exception;
            } else {
                throw new ExecutionException(exception);
            }
        }
        if (!done) {
            throw new TimeoutException();
        }
        return result;
    }

    @Override
    protected void set(V value) {
    }

    public void setExecutionCallback(ExecutionCallback<V> executionCallback) {
        inner.setExecutionCallback(executionCallback);
    }

    public ExecutionCallback<V> getExecutionCallback() {
        return inner.getExecutionCallback();
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (done || cancelled)
            return false;
        cancelled = inner.cancel(mayInterruptIfRunning);
        return cancelled;
    }

    protected void setMemberLeft(Member member) {
    }

    public Object getInner() {
        return inner;
    }

    public static <V> Callable<V> callable(Runnable task, V result) {
        return new DistributedRunnableAdapterImpl<V>(task, result);
    }

    protected void onResult(V result) {
        this.result = result;
    }

    protected class Inner implements InnerFutureTask<V> {
        private final Callable<V> callable;

        private final Member member;

        private final Set<Member> members;

        private final Object key;

        /**
         * A callback to the ExecutionManager running the task.
         * When nulled after set/cancel, indicates that
         * the results are accessible.
         * Declared volatile to ensure visibility upon completion.
         */
        private volatile ExecutionManagerCallback executionManagerCallback;
        // user execution callback
        private ExecutionCallback<V> executionCallback = null;

        public Inner(Callable<V> callable, Member member, Set<Member> members, Object key) {
            super();
            this.callable = callable;
            this.member = member;
            this.key = key;
            this.members = members;
        }

        public void get() throws InterruptedException, ExecutionException {
            executionManagerCallback.get();
        }

        public void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            executionManagerCallback.get(timeout, unit);
        }

        public void innerDone() {
            done = true;
            done();
        }

        public boolean isDone() {
            return done;
        }

        public void innerSet(V value) {
            set(value);
            onResult(value);
        }

        public void innerSetException(Throwable throwable, boolean done) {
            if (done) {
                innerDone();
            }
            exception = throwable;
            setException(throwable);
        }

        public void innerSetCancelled() {
            cancelled = true;
            innerDone();
        }

        public void innerSetMemberLeft(Member member) {
            innerDone();
            memberLeftException = new MemberLeftException(member);
            setMemberLeft(member);
        }

        public Callable<V> getCallable() {
            return callable;
        }

        public Object getKey() {
            return key;
        }

        public Member getMember() {
            return member;
        }

        public Set<Member> getMembers() {
            return members;
        }

        public void setExecutionManagerCallback(ExecutionManagerCallback executionManagerCallback) {
            this.executionManagerCallback = executionManagerCallback;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            if (executionManagerCallback == null) {
                return false;
            }
            cancelled = executionManagerCallback.cancel(mayInterruptIfRunning);
            if (cancelled) {
                innerDone();
            }
            return cancelled;
        }

        public ExecutionCallback getExecutionCallback() {
            return executionCallback;
        }

        public void setExecutionCallback(ExecutionCallback<V> executionCallback) {
            this.executionCallback = executionCallback;
        }
    }

    public static class DistributedRunnableAdapterImpl<V> implements DistributedRunnableAdapter,
            Serializable, Callable<V>, HazelcastInstanceAware {

        private static final long serialVersionUID = -4;

        private Runnable task;

        private V result;

        public DistributedRunnableAdapterImpl(Runnable task, V result) {
            super();
            this.task = task;
            this.result = result;
        }

        public V getResult() {
            return result;
        }

        public Runnable getRunnable() {
            return task;
        }

        public void setRunnable(Runnable runnable) {
            task = runnable;
        }

        public V call() {
            task.run();
            return result;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            if (task instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) task).setHazelcastInstance(hazelcastInstance);
            }
        }
    }
}
