/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.*;
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
        ClientMessage request = ConditionBeforeAwaitParameters.encode(conditionId, threadId, lockProxy.getName());
        invoke(request);
    }

    private boolean doAwait(long time, TimeUnit unit, long threadId) throws InterruptedException {
        final long timeoutInMillis = unit.toMillis(time);
        ClientMessage request = ConditionAwaitParameters.encode(conditionId, timeoutInMillis, threadId, lockProxy.getName());
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }


    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        long until = deadline.getTime();
        final long timeToDeadline = until - Clock.currentTimeMillis();
        return await(timeToDeadline, TimeUnit.MILLISECONDS);
    }

    @Override
    public void signal() {
        ClientMessage request = ConditionSignalParameters.encode(conditionId, ThreadUtil.getThreadId(), lockProxy.getName());
        invoke(request);
    }

    @Override
    public void signalAll() {
        ClientMessage request = ConditionSignalAllParameters.encode(conditionId, ThreadUtil.getThreadId(), lockProxy.getName());
        invoke(request);
    }

    protected <T> T invoke(ClientRequest req) {
        return super.invoke(req, key);
    }

}
