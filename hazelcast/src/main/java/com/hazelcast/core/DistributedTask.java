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

package com.hazelcast.core;

import static com.hazelcast.impl.Constants.Objects.*;
import com.hazelcast.impl.DistributedRunnableAdapter;
import com.hazelcast.impl.ExecutionManagerCallback;
import com.hazelcast.impl.InnerFutureTask;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A cancellable asynchronous distributed computation.
 * 
 * This class is analogue to java.util.concurrent.FutureTask.
 * 
 * Once the computation has completed, the computation cannot
 * be restarted or cancelled.
 */
public class DistributedTask<V> extends FutureTask<V> {
    private volatile V result = null;

    private volatile Throwable exception = null;

    private Inner inner = null;

    private volatile boolean done = false;

    private volatile boolean cancelled = false;

    private volatile boolean memberLeft = false;

    private DistributedTask(Callable<V> callable, Member member, Set<Member> members, Object key) {
        super(callable);
        if (callable instanceof DistributedRunnableAdapter) {
            DistributedRunnableAdapter<V> dra = (DistributedRunnableAdapter<V>) callable;
            this.result = dra.getResult();
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
    public V get() throws InterruptedException, ExecutionException, MemberLeftException {
   		inner.get();
        if (cancelled)
            throw new CancellationException();
        if (exception != null)
            throw new ExecutionException(exception);
        if (memberLeft)
            throw new MemberLeftException();
        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException, MemberLeftException {
        inner.get(timeout, unit);
        if (cancelled)
            throw new CancellationException();
        if (exception != null)
            throw new ExecutionException(exception);
        if (memberLeft)
            throw new MemberLeftException();
        return result;
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

    @Override
    protected void done() {
    }

    @Override
    protected void set(V result) {
        this.result = result;
    }

    @Override
    protected void setException(Throwable throwable) {
        this.exception = throwable;
    }

    protected void setMemberLeft(Member member) {
    }

    public Object getInner() {
        return inner;
    }

    public static <V> Callable<V> callable(Runnable task, V result) {
        return new DistributedRunnableAdapterImpl<V>(task, result);
    }

    private class Inner implements InnerFutureTask<V> {
        private final Callable<V> callable;

        private final Member member;

        private final Set<Member> members;

        private final Object key;
        
        /**
         * A clalback to the ExecutionManager running the task.
         * When nulled after set/cancel, indicates that
         * the results are accessible.
         * Declared volatile to ensure visibility upon completion.
         */
        private volatile ExecutionManagerCallback executionManagerCallback;

        private ExecutionCallback<V> executionCallback = null; // user
        // executioncallback

        public Inner(Callable<V> callable, Member member, Set<Member> members, Object key) {
            super();
            this.callable = callable;
            this.member = member;
            this.key = key;
            this.members = members;
        }

        public void innerDone() {
            done = true;
            done();
        }

        public V get() throws InterruptedException, ExecutionException {
        	if (executionManagerCallback == null) {
        		return null;
        	}
            Object r = null;
            while ((r = executionManagerCallback.get()) != OBJECT_DONE) {
            	if (r == OBJECT_DONE) {
            		// nothing to do
                } else if (r == OBJECT_CANCELLED) {
                    cancelled = true;
                } else if (r == OBJECT_MEMBER_LEFT) {
                    memberLeft = true;
                } else if (r instanceof Throwable) {
                    innerSetException((Throwable) r);
                } else {
                    if (r == OBJECT_NULL) {
                        innerSet(null);
                    } else {
                        innerSet((V) r);
                    }
                }
            }
            executionManagerCallback = null;
            return null;
        }

        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
        	if (executionManagerCallback == null) {
        		return null;
        	}
            long timeoutMillis = unit.toMillis(timeout);
            long start = System.currentTimeMillis();
            long timeLeft = timeoutMillis;
            Object r = null;
            while ((r = executionManagerCallback.get(timeLeft, TimeUnit.MILLISECONDS)) != OBJECT_DONE) {
                if (r == null) {
                    throw new TimeoutException();
                } else if (r == OBJECT_DONE) {
            		// nothing to do
                } else if (r == OBJECT_CANCELLED) {
                    cancelled = true;
                } else if (r instanceof Throwable) {
                    innerSetException((Throwable) r);
                } else {
                    if (r == OBJECT_NULL) {
                        innerSet(null);
                    } else {
                        innerSet((V) r);
                    }
                }
                timeLeft = timeoutMillis - (System.currentTimeMillis() - start);
            }
            executionManagerCallback = null;
            return null;
        }

        public void innerSet(V value) {
            set(value);
            innerDone();
        }

        public void innerSetException(Throwable throwable) {
            setException(throwable);
            innerDone();
        }

        public void innerSetMemberLeft(Member member) {
            setMemberLeft(member);
            innerDone();
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
            if (cancelled)
                innerDone();
            // executionManagerCallback = null; Should be restartable?
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
            Serializable, Callable<V> {

        private static final long serialVersionUID = -2297288043381543510L;

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

        public V call() {
            task.run();
            return result;
        }
    }
}
