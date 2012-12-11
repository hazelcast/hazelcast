/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.util.Util;

import java.util.concurrent.*;
import java.util.logging.Level;

abstract class InvocationImpl implements Future, Invocation, Callback {
    private static final Object NULL = new Object();
    private static final Object RETRY = new Object();

    private final BlockingQueue<Object> responseQ = new LinkedBlockingQueue<Object>();
    protected final NodeServiceImpl nodeService;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final int replicaIndex;
    protected final int tryCount;
    protected final long tryPauseMillis;
    private volatile int invokeCount = 0;
    private volatile boolean done = false;

    InvocationImpl(NodeServiceImpl nodeService, String serviceName, Operation op, int partitionId,
                   int replicaIndex, int tryCount, long tryPauseMillis) {
        this.nodeService = nodeService;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
    }

    public void notify(Object result) {
//        if (response.isException()) {
//            setResult(IOUtil.toObject(result));
//        } else {
//            setResult(result);
//        }
        setResult(result);
    }

    protected abstract Address getTarget();

    public final Future invoke() {
        try {
            invokeCount++;
            nodeService.invoke(this);
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                setResult(e);
            } else {
                Util.throwUncheckedException(e);
            }
        }
        return this;
    }

    final void setResult(Object obj) {
        if (obj == null) {
            obj = NULL;
        }
        responseQ.offer(obj);
    }

    public Object get() throws InterruptedException, ExecutionException {
        try {
            return doGet(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            nodeService.getLogger(getClass().getName()).log(Level.FINEST, e.getMessage(), e);
            return null;
        }
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return doGet(timeout, unit);
    }

    // TODO: rethink about interruption and timeouts
    private Object doGet(long time, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        long timeout = unit.toMillis(time);
        if (timeout < 0) timeout = 0;

        while (timeout >= 0) {
            final Object response = pollResponse(timeout);
            if (response == NULL) {
                return null;
            } else if (response == RETRY) {
                Thread.sleep(tryPauseMillis);
                if (timeout != Long.MAX_VALUE) {
                    timeout -= tryPauseMillis;
                }
                if (timeout > 0) {
                    invoke();
                }
            } else {
                return response;
            }
        }
        throw new TimeoutException();
    }

    // TODO: rethink about interruption and timeouts
    private Object pollResponse(final long timeout) throws ExecutionException, InterruptedException {
        final Object obj = (timeout == Long.MAX_VALUE) ? responseQ.take() : responseQ.poll(timeout, TimeUnit.MILLISECONDS);
        if (obj instanceof RetryableException) {
            if (invokeCount < tryCount) {
                return RETRY;
            } else {
                done = true;
                throw new ExecutionException((Throwable) obj);
            }
        } else {
            done = true;
            if (obj instanceof Throwable) {
                throw new ExecutionException((Throwable) obj);
            } else {
                return obj;
            }
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public Operation getOperation() {
        return op;
    }

    public PartitionInfo getPartitionInfo() {
        return nodeService.getPartitionInfo(partitionId);
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    public boolean isCancelled() {
        return false;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isDone() {
        return done;
    }

    @Override
    public String toString() {
        return "InvocationImpl{" +
                "serviceName='" + serviceName + '\'' +
                ", op=" + op +
                ", partitionId=" + partitionId +
                ", replicaIndex=" + replicaIndex +
                '}';
    }
}
