/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Future;

/**
 * Abstract {@link MultiPartitionOperation} which takes care about the partitions and the response of the operation.
 */
public abstract class AbstractMultiPartitionOperation extends MapOperation implements MultiPartitionOperation, DataSerializable {

    private int[] partitions;

    private transient Object[] results;

    @SuppressWarnings("unused")
    public AbstractMultiPartitionOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public AbstractMultiPartitionOperation(String name, int[] partitions) {
        super(name);
        this.partitions = partitions;
    }

    protected int getPartitionCount() {
        return partitions.length;
    }

    protected abstract void doRun(int[] partitions, Future[] futures);

    protected abstract Operation createFailureOperation(int failedPartitionId, int partitionIndex);

    @Override
    public final void run() throws Exception {
        Future[] futures = new Future[partitions.length];
        doRun(partitions, futures);
        // TODO: this should be done non-blocking to free the operation thread as soon as possible
        results = new Object[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            try {
                results[i] = futures[i].get();
            } catch (Throwable t) {
                results[i] = t;
            }
        }
    }

    @Override
    public final Object getResponse() {
        return new PartitionResponse(partitions, results);
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public final int[] getPartitions() {
        return partitions;
    }

    @Override
    public final Operation createFailureOperation(int failedPartitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == failedPartitionId) {
                return createFailureOperation(failedPartitionId, i);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + failedPartitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeIntArray(partitions);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        partitions = in.readIntArray();
    }
}
