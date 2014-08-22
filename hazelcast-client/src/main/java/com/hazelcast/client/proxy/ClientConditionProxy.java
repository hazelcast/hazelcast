package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.client.AwaitRequest;
import com.hazelcast.concurrent.lock.client.BeforeAwaitRequest;
import com.hazelcast.concurrent.lock.client.SignalRequest;
import com.hazelcast.core.ICondition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ClientConditionProxy extends ClientProxy implements ICondition {

    private final String conditionId;
    private final ClientLockProxy lockProxy;

    private volatile Data key;
    private final InternalLockNamespace namespace;

    public ClientConditionProxy(ClientLockProxy clientLockProxy, String name, ClientContext ctx) {
        super(LockService.SERVICE_NAME, clientLockProxy.getName());
        this.setContext(ctx);
        this.lockProxy = clientLockProxy;
        this.namespace = new InternalLockNamespace(lockProxy.getName());
        this.conditionId = name;
        this.key = toData(lockProxy.getName());
    }

    @Override
    public void await() throws InterruptedException {
        await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void awaitUninterruptibly() {
        try {
            await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // TODO: @mm - what if interrupted?
            ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        long start = System.nanoTime();
        await(nanosTimeout, TimeUnit.NANOSECONDS);
        long end = System.nanoTime();
        return (end - start);
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        long threadId = ThreadUtil.getThreadId();
        beforeAwait(threadId);
        return doAwait(time, unit, threadId);
    }

    private void beforeAwait(long threadId) {
        BeforeAwaitRequest request = new BeforeAwaitRequest(namespace, threadId, conditionId, key);
        invoke(request);

    }

    private boolean doAwait(long time, TimeUnit unit, long threadId) throws InterruptedException {
        final long timeoutInMillis = unit.toMillis(time);
        AwaitRequest awaitRequest = new AwaitRequest(namespace, lockProxy.getName(), timeoutInMillis, threadId, conditionId);
        final Boolean result = invoke(awaitRequest);
        return result;
    }


    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        long until = deadline.getTime();
        final long timeToDeadline = until - Clock.currentTimeMillis();
        return await(timeToDeadline, TimeUnit.MILLISECONDS);
    }

    @Override
    public void signal() {
        signal(false);
    }

    @Override
    public void signalAll() {
        signal(true);
    }

    private void signal(boolean all) {
        SignalRequest request = new SignalRequest(namespace, lockProxy.getName(), ThreadUtil.getThreadId(), conditionId, all);
        invoke(request);
    }

    protected <T> T invoke(ClientRequest req) {
        return super.invoke(req, key);
    }

}
