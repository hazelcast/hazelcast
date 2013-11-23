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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.core.AsyncCountDownLatch;
import com.hazelcast.core.CompletionFuture;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.spi.*;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 1/10/13
 */
public class CountDownLatchProxy extends AbstractDistributedObject<CountDownLatchService> implements AsyncCountDownLatch {

    private final String name;
    private final int partitionId;

    public CountDownLatchProxy(String name, NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.name = name;
        partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CountDownLatchService.SERVICE_NAME,
                new AwaitOperation(name, getTimeInMillis(timeout, unit)), partitionId).build();
        try {
            return nodeEngine.toObject(inv.invoke().get());
        } catch (ExecutionException e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }

    @Override
    public CompletionFuture<Boolean> asyncAwait(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    @Override
    public void countDown() {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CountDownLatchService.SERVICE_NAME,
                new CountDownOperation(name), partitionId).build();
        try {
            inv.invoke().get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public CompletionFuture<Void> asyncCountDown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCount() {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CountDownLatchService.SERVICE_NAME,
                new GetCountOperation(name), partitionId).build();
        try {
            return nodeEngine.toObject(inv.invoke().get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public CompletionFuture<Integer> asyncGetCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean trySetCount(int count) {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CountDownLatchService.SERVICE_NAME,
                new SetCountOperation(name, count), partitionId).build();
        try {
            return nodeEngine.toObject(inv.invoke().get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public CompletionFuture<Boolean> asyncTrySetCount(int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ICountDownLatch{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
