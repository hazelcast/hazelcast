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
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType.AFTER_VALUE;
import static com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType.BEFORE_VALUE;

public class RaftAtomicLongProxy implements IAtomicLong {

    private final String name;
    private final RaftGroupId groupId;
    private final RaftInvocationManager raftInvocationManager;

    public RaftAtomicLongProxy(String name, RaftGroupId groupId, RaftInvocationManager invocationManager) {
        this.name = name;
        this.groupId = groupId;
        this.raftInvocationManager = invocationManager;
    }

    @Override
    public long addAndGet(long delta) {
        ICompletableFuture<Long> future = addAndGetAsync(delta);
        return join(future);
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
        ICompletableFuture<Boolean> future = compareAndSetAsync(expect, update);
        return join(future);
    }

    @Override
    public long getAndAdd(long delta) {
        ICompletableFuture<Long> future = getAndAddAsync(delta);
        return join(future);
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
        ICompletableFuture<Long> future = getAndSetAsync(newValue);
        return join(future);
    }

    @Override
    public void set(long newValue) {
        getAndSet(newValue);
    }

    private <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<Long> addAndGetAsync(final long delta) {
        return raftInvocationManager.invoke(groupId, new AddAndGetOp(name, delta));
    }

    @Override
    public ICompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public ICompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public ICompletableFuture<Boolean> compareAndSetAsync(final long expect, final long update) {
        return raftInvocationManager.invoke(groupId, new CompareAndSetOp(name, expect, update));
    }

    @Override
    public ICompletableFuture<Long> getAndAddAsync(final long delta) {
        return raftInvocationManager.invoke(groupId, new GetAndAddOp(name, delta));
    }

    @Override
    public ICompletableFuture<Long> getAsync() {
        return getAndAddAsync(0);
    }

    @Override
    public ICompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public ICompletableFuture<Long> getAndSetAsync(final long newValue) {
        return raftInvocationManager.invoke(groupId, new GetAndSetOp(name, newValue));
    }

    @Override
    public ICompletableFuture<Void> setAsync(long newValue) {
        ICompletableFuture future = getAndSetAsync(newValue);
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
        ICompletableFuture<Long> future = doAlterAsync(function, alterResultType);
        return join(future);
    }

    private ICompletableFuture<Long> doAlterAsync(final IFunction<Long, Long> function, final AlterResultType alterResultType) {
        return raftInvocationManager.invoke(groupId, new AlterOp(name, function, alterResultType));
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        ICompletableFuture<R> future = applyAsync(function);
        return join(future);
    }

    @Override
    public ICompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        ICompletableFuture future = doAlterAsync(function, AFTER_VALUE);
        return future;
    }

    @Override
    public ICompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        return doAlterAsync(function, AFTER_VALUE);
    }

    @Override
    public ICompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        return doAlterAsync(function, BEFORE_VALUE);
    }

    @Override
    public <R> ICompletableFuture<R> applyAsync(final IFunction<Long, R> function) {
        return raftInvocationManager.invoke(groupId, new ApplyOp<R>(name, function));
    }

    public long localGet(QueryPolicy queryPolicy) {
        return join(localGetAsync(queryPolicy));
    }

    public ICompletableFuture<Long> localGetAsync(final QueryPolicy queryPolicy) {
        final SimpleCompletableFuture<Long> resultFuture = new SimpleCompletableFuture<Long>(null, null);
        ICompletableFuture<Long> localFuture =
                raftInvocationManager.queryOnLocal(groupId, new LocalGetOp(name), queryPolicy);

        localFuture.andThen(new ExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                resultFuture.setResult(response);
            }

            @Override
            public void onFailure(Throwable t) {
                ICompletableFuture<Long> future = raftInvocationManager.query(groupId, new LocalGetOp(name), queryPolicy);
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
        join(raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)));
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return "RaftAtomicLongProxy{" + "name='" + name + '\'' + ", groupId=" + groupId + '}';
    }
}
