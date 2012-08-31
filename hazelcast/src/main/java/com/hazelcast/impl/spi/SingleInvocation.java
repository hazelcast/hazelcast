/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.spi;

import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.nio.Address;

import java.util.concurrent.*;

abstract class SingleInvocation implements Future, Invocation, Callback {
    private final static Object NULL = new Object();
    private final BlockingQueue responseQ = new LinkedBlockingQueue();
    protected final NodeServiceImpl nodeService;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected int replicaIndex = 0;
    protected int tryCount = 100;
    protected long tryPauseMillis = 500;
    protected volatile int invokeCount = 0;
    private volatile boolean done = false;

    SingleInvocation(NodeServiceImpl nodeService, String serviceName, Operation op, int partitionId,
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
        if (result instanceof Response) {
            Response response = (Response) result;
            if (response.isException()) {
                setResult(response.getResult());
            } else {
                setResult(response.getResultData());
            }
        } else {
            setResult(result);
        }
    }

    abstract Address getTarget();

    public Future invoke() {
        try {
            invokeCount++;
            nodeService.invokeSingle(this);
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                setResult(e);
            } else {
                throw (RuntimeException) e;
            }
        }
        return this;
    }

    void setResult(Object obj) {
        if (obj == null) {
            obj = NULL;
        }
        responseQ.offer(obj);
    }

    private Object doGet(long time, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        long timeout = unit.toMillis(time);
        if (timeout < 0) timeout = 0;
        Object obj = (timeout == Long.MAX_VALUE) ? responseQ.take() : responseQ.poll(timeout, TimeUnit.MILLISECONDS);
        if (obj == NULL) obj = null;
        if (obj instanceof RetryableException) {
            if (invokeCount < tryCount) {
                Thread.sleep(tryPauseMillis);
                if (timeout != Long.MAX_VALUE) {
                    timeout -= tryPauseMillis;
                }
                invoke();
                return doGet(timeout, TimeUnit.MILLISECONDS);
            } else {
                done = true;
                throw new ExecutionException((Throwable) obj);
            }
        } else {
            done = true;
            if (obj instanceof Exception) {
                throw new ExecutionException((Throwable) obj);
            } else {
                return obj;
            }
        }
    }

    public Object get() throws InterruptedException, ExecutionException {
        try {
            return doGet(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return doGet(timeout, unit);
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
        return "SingleInvocation{" +
                "serviceName='" + serviceName + '\'' +
                ", op=" + op +
                ", partitionId=" + partitionId +
                ", replicaIndex=" + replicaIndex +
                '}';
    }
}
