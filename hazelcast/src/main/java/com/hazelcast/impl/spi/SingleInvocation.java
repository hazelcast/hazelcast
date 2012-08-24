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

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

abstract class SingleInvocation extends FutureTask implements Invocation, Callback {
    protected final NodeService nodeService;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected int replicaIndex = 0;
    protected int tryCount = 100;
    protected long tryPauseMillis = 500;
    protected volatile int invokeCount = 0;

    SingleInvocation(NodeService nodeService, String serviceName, Operation op, int partitionId,
                     int replicaIndex, int tryCount, long tryPauseMillis) {
        super(op, null);
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
            setResult(e);
        }
        return this;
    }

    abstract void setResult(Object obj) ;

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
}
