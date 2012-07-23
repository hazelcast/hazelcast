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

public class SinglePartitionInvocationBuilder {

    private final NodeService nodeService;
    private final String serviceName;
    private final Operation op;
    private final PartitionInfo partitionInfo;
    private int replicaIndex = 0;
    private int tryCount = 100;
    private long tryPauseMillis = 500;

    public SinglePartitionInvocationBuilder(NodeService nodeService, String serviceName, Operation op, PartitionInfo partitionInfo) {
        this.nodeService = nodeService;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionInfo = partitionInfo;
    }

    public SinglePartitionInvocationBuilder(NodeService nodeService, String serviceName, Operation op, PartitionInfo partitionInfo, int replicaIndex, int tryCount, long tryPauseMillis) {
        this.nodeService = nodeService;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionInfo = partitionInfo;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
    }

    public SinglePartitionInvocationBuilder setReplicaIndex(int replicaIndex) {
        this.replicaIndex = replicaIndex;
        return this;
    }

    public SinglePartitionInvocationBuilder setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    public SinglePartitionInvocationBuilder setTryPauseMillis(long tryPauseMillis) {
        this.tryPauseMillis = tryPauseMillis;
        return this;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Operation getOp() {
        return op;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getTryCount() {
        return tryCount;
    }

    public long getTryPauseMillis() {
        return tryPauseMillis;
    }

    public Invocation build() {
        return new SinglePartitionInvocation(nodeService, serviceName, op, partitionInfo, replicaIndex, tryCount, tryPauseMillis);
    }
}
