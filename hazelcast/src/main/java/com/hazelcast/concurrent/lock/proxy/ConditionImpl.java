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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;

/**
 * @author mdogan 2/13/13
 */
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
        final long start = System.nanoTime();
        await(nanosTimeout, TimeUnit.NANOSECONDS);
        final long end = System.nanoTime();
        return (end - start);
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        final NodeEngine nodeEngine = lockProxy.getNodeEngine();
        final long threadId = ThreadUtil.getThreadId();

        try {
            BeforeAwaitOperation op = new BeforeAwaitOperation(namespace, lockProxy.key, threadId, conditionId);
            Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, op,partitionId);
            f.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
        try {
            AwaitOperation op = new AwaitOperation(namespace, lockProxy.key, threadId, unit.toMillis(time), conditionId);
            Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, op, partitionId);
            return Boolean.TRUE.equals(f.get());
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        final long until = deadline.getTime();
        return await(until - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void signal() {
        signal(false);
    }

    private void signal(boolean all) {
        final NodeEngine nodeEngine = lockProxy.getNodeEngine();
        Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME,
                new SignalOperation(namespace, lockProxy.key, ThreadUtil.getThreadId(), conditionId, all), partitionId);
        try {
            f.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public void signalAll() {
        signal(true);
    }
}
