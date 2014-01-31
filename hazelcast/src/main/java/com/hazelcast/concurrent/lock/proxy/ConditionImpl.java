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

package com.hazelcast.concurrent.lock.proxy;

import com.hazelcast.concurrent.lock.AwaitOperation;
import com.hazelcast.concurrent.lock.BeforeAwaitOperation;
import com.hazelcast.concurrent.lock.SignalOperation;
import com.hazelcast.core.ICondition;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;

final class ConditionImpl implements ICondition {

    private final LockProxy lockProxy;
    private final int partitionId;
    private final String conditionId;
    private final ObjectNamespace namespace;

    public ConditionImpl(LockProxy lockProxy, String id) {
        this.lockProxy = lockProxy;
        this.partitionId = lockProxy.getPartitionId();
        this.conditionId = id;
        this.namespace = lockProxy.getNamespace();
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

    private boolean doAwait(long time, TimeUnit unit, long threadId) throws InterruptedException {
        try {
            AwaitOperation op = new AwaitOperation(namespace, lockProxy.key, threadId, unit.toMillis(time), conditionId);
            Future f = invoke(op);
            return Boolean.TRUE.equals(f.get());
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    private void beforeAwait(long threadId) {
        BeforeAwaitOperation op = new BeforeAwaitOperation(namespace, lockProxy.key, threadId, conditionId);
        InternalCompletableFuture f = invoke(op);
        f.getSafely();
    }

    private InternalCompletableFuture invoke(Operation op) {
        NodeEngine nodeEngine = lockProxy.getNodeEngine();
        return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, op, partitionId);
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        long until = deadline.getTime();
        return await(until - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void signal() {
        signal(false);
    }

    private void signal(boolean all) {
        SignalOperation op = new SignalOperation(namespace, lockProxy.key, ThreadUtil.getThreadId(), conditionId, all);
        InternalCompletableFuture f = invoke(op);
        f.getSafely();
    }

    @Override
    public void signalAll() {
        signal(true);
    }
}
