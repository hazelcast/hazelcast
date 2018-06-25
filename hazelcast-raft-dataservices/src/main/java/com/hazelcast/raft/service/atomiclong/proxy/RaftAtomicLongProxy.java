/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.raft.service.atomiclong.operation.AddAndGetOp;
import com.hazelcast.raft.service.atomiclong.operation.AlterOp;
import com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType;
import com.hazelcast.raft.service.atomiclong.operation.ApplyOp;
import com.hazelcast.raft.service.atomiclong.operation.CompareAndSetOp;
import com.hazelcast.raft.service.atomiclong.operation.GetAndAddOp;
import com.hazelcast.raft.service.atomiclong.operation.GetAndSetOp;
import com.hazelcast.raft.service.atomiclong.operation.LocalGetOp;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

import static com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType.AFTER_VALUE;
import static com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType.BEFORE_VALUE;

/**
 * Raft-based proxy implementation of {@link IAtomicLong} interface
 */
@SuppressWarnings("checkstyle:methodcount")
public class RaftAtomicLongProxy implements IAtomicLong {

    private final String name;
    private final RaftGroupId groupId;
    private final RaftInvocationManager invocationManager;

    public RaftAtomicLongProxy(RaftInvocationManager invocationManager, RaftGroupId groupId, String name) {
        this.name = name;
        this.groupId = groupId;
        this.invocationManager = invocationManager;
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).join();
    }

    @Override
    public long incrementAndGet() {
        return addAndGet(1);
    }

    @Override
    public long decrementAndGet() {
        return addAndGet(-1);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).join();
    }

    @Override
    public long get() {
        return getAndAdd(0);
    }

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public void set(long newValue) {
        getAndSet(newValue);
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(final long delta) {
        return invocationManager.invoke(groupId, new AddAndGetOp(name, delta));
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(final long expect, final long update) {
        return invocationManager.invoke(groupId, new CompareAndSetOp(name, expect, update));
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(final long delta) {
        return invocationManager.invoke(groupId, new GetAndAddOp(name, delta));
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        return getAndAddAsync(0);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(final long newValue) {
        return invocationManager.invoke(groupId, new GetAndSetOp(name, newValue));
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        InternalCompletableFuture future = getAndSetAsync(newValue);
        return future;
    }

    @Override
    public void alter(final IFunction<Long, Long> function) {
        doAlter(function, AFTER_VALUE);
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return doAlter(function, AFTER_VALUE);
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return doAlter(function, BEFORE_VALUE);
    }

    private long doAlter(IFunction<Long, Long> function, AlterResultType alterResultType) {
        return doAlterAsync(function, alterResultType).join();
    }

    private InternalCompletableFuture<Long> doAlterAsync(IFunction<Long, Long> function, AlterResultType alterResultType) {
        return invocationManager.invoke(groupId, new AlterOp(name, function, alterResultType));
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        InternalCompletableFuture future = doAlterAsync(function, AFTER_VALUE);
        return future;
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        return doAlterAsync(function, AFTER_VALUE);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        return doAlterAsync(function, BEFORE_VALUE);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(final IFunction<Long, R> function) {
        return invocationManager.invoke(groupId, new ApplyOp<R>(name, function));
    }

    public long localGet(QueryPolicy queryPolicy) {
        Future<Long> f = localGetAsync(queryPolicy);
        try {
            return f.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public ICompletableFuture<Long> localGetAsync(final QueryPolicy queryPolicy) {
        final SimpleCompletableFuture<Long> resultFuture = new SimpleCompletableFuture<Long>(null, null);
        ICompletableFuture<Long> localFuture =
                invocationManager.queryOnLocal(groupId, new LocalGetOp(name), queryPolicy);

        localFuture.andThen(new ExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                resultFuture.setResult(response);
            }

            @Override
            public void onFailure(Throwable t) {
                ICompletableFuture<Long> future = invocationManager.query(groupId, new LocalGetOp(name), queryPolicy);
                future.andThen(new ExecutionCallback<Long>() {
                    @Override
                    public void onResponse(Long response) {
                        resultFuture.setResult(response);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        resultFuture.setResult(t);
                    }
                });

            }
        });
        return resultFuture;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return "RaftAtomicLongProxy{" + "name='" + name + '\'' + ", groupId=" + groupId + '}';
    }
}
