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

package com.hazelcast.concurrent.lock;

import com.hazelcast.core.ICondition;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 2/13/13
 */
public class ConditionImpl implements ICondition {

    private final LockProxy lockProxy;
    private final int partitionId;

    public ConditionImpl(LockProxy lockProxy) {
        this.lockProxy = lockProxy;
        this.partitionId = lockProxy.getNodeEngine().getPartitionService().getPartitionId(lockProxy.key);
    }

    public void await() throws InterruptedException {
        await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void awaitUninterruptibly() {
        try {
            await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            ExceptionUtil.sneakyThrow(e);
        }
    }

    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        final long start = System.nanoTime();
        await(nanosTimeout, TimeUnit.NANOSECONDS);
        final long end = System.nanoTime();
        return (end - start);
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        final NodeEngine nodeEngine = lockProxy.getNodeEngine();
        final Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(LockService.SERVICE_NAME,
                new AwaitOperation(lockProxy.namespace, lockProxy.key, ThreadContext.getThreadId()), partitionId).build();
        Future f = inv.invoke();
        try {
            return (Boolean) f.get(time, unit);
        } catch (Throwable t) {
            return (Boolean) ExceptionUtil.rethrow(t);
        }
    }

    public boolean awaitUntil(Date deadline) throws InterruptedException {
        final long until = deadline.getTime();
        return await(until - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public void signal() {

    }

    public void signalAll() {

    }
}
