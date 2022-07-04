/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.spi.impl.operationservice.CallStatus.OFFLOAD_ORDINAL;
import static com.hazelcast.spi.impl.operationservice.CallStatus.RESPONSE;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

public class QueryOperation extends AbstractNamedOperation implements ReadonlyOperation {

    private Query query;

    private transient Result result;
    private transient CallStatus callStatus;

    public QueryOperation() {
    }

    public QueryOperation(Query query) {
        super(query.getMapName());
        this.query = query;
    }

    private InMemoryFormat getMapInMemoryFormat() {
        MapContainer mapContainer = getMapServiceContext().getMapContainer(name);
        return mapContainer.getMapConfig().getInMemoryFormat();
    }

    private MapServiceContext getMapServiceContext() {
        MapService mapService = getService();
        return mapService.getMapServiceContext();
    }

    @Override
    public CallStatus call() throws Exception {
        callStatus = callInternal();
        return callStatus;
    }

    @Nonnull
    private CallStatus callInternal() {
        QueryRunner queryRunner = getMapServiceContext().getMapQueryRunner(getName());

        switch (getMapInMemoryFormat()) {
            case BINARY:
            case OBJECT:
                result = queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
                return RESPONSE;
            case NATIVE:
                boolean useGlobalIndex = getMapServiceContext().getMapContainer(getName()).shouldUseGlobalIndex();

                if (useGlobalIndex) {
                    // Try to use HD global index
                    // Don't do map scan because it is not thread-safe
                    Result indexResult = queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query, false);
                    if (indexResult != null) {
                        result = indexResult;
                        return RESPONSE;
                    }
                }

                // Offload query run on the partition threads.
                BitSet queryPartitions = localPartitions();
                if (query.getPartitionIdSet() != null) {
                    queryPartitions.and(query.getPartitionIdSet().bitSetCopy());
                }
                if (queryPartitions.cardinality() == 0) {
                    // important to deal with situation of not having any partitions
                    result = queryRunner.populateEmptyResult(query, Collections.emptyList());
                    return RESPONSE;
                } else {
                    return new OffloadedImpl(queryRunner, queryPartitions);
                }
            default:
                throw new IllegalArgumentException("Unsupported in memory format");
        }
    }

    private int partitionCount() {
        return getNodeEngine().getPartitionService().getPartitionCount();
    }

    private OperationServiceImpl getOperationService() {
        return (OperationServiceImpl) getNodeEngine().getOperationService();
    }

    private BitSet localPartitions() {
        BitSet partitions = new BitSet(partitionCount());
        for (IPartition partition : getNodeEngine().getPartitionService().getPartitions()) {
            if (partition.isLocal()) {
                partitions.set(partition.getPartitionId());
            }
        }

        return partitions;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (callStatus != null
                && callStatus.ordinal() == OFFLOAD_ORDINAL) {
            // This is required since if the returnsResponse() method returns false
            // there won't be any response sent to the invoking party - this means
            // that the operation won't be retried if the exception is instanceof
            // HazelcastRetryableException
            sendResponse(e);
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.QUERY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(query);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        query = in.readObject();
    }

    private final class OffloadedImpl extends Offload {
        private final BitSet localPartitions;
        private final QueryRunner queryRunner;

        private OffloadedImpl(QueryRunner queryRunner, BitSet localPartitions) {
            super(QueryOperation.this);
            this.localPartitions = localPartitions;
            this.queryRunner = queryRunner;
        }

        @Override
        public void start() {
            QueryFuture future = new QueryFuture(localPartitions.cardinality());
            getOperationService().executeOnPartitions(
                    new QueryTaskFactory(query, queryRunner, future), localPartitions);
            future.whenCompleteAsync(new ExecutionCallbackImpl(queryRunner, query));
        }
    }

    private class QueryFuture extends InternalCompletableFuture {
        private final AtomicReferenceArray<Result> resultArray = new AtomicReferenceArray<Result>(partitionCount());
        private final AtomicInteger remaining;

        QueryFuture(int localPartitionCount) {
            this.remaining = new AtomicInteger(localPartitionCount);
        }

        void addResult(int partitionId, Result result) {
            if (result != null) {
                resultArray.set(partitionId, result);
            }

            if (remaining.decrementAndGet() == 0) {
                complete(resultArray);
            }
        }
    }

    private class QueryTaskFactory implements PartitionTaskFactory {
        private final Query query;
        private final QueryFuture future;
        private final QueryRunner queryRunner;

        QueryTaskFactory(Query query, QueryRunner queryRunner, QueryFuture future) {
            this.query = query;
            this.queryRunner = queryRunner;
            this.future = future;
        }

        @Override
        public Object create(int partitionId) {
            return new QueryTask(query, queryRunner, partitionId, future);
        }
    }

    private class QueryTask implements Runnable {

        private final int partitionId;
        private final Query query;
        private final QueryFuture future;
        private final QueryRunner queryRunner;

        QueryTask(Query query, QueryRunner queryRunner,
                  int partitionId, QueryFuture future) {
            this.query = query;
            this.queryRunner = queryRunner;
            this.partitionId = partitionId;
            this.future = future;
        }

        @Override
        public void run() {
            IPartition partition = getNodeEngine().getPartitionService().getPartition(partitionId);
            if (!partition.isLocal()) {
                future.addResult(partitionId, null);
                return;
            }

            try {
                Result result
                        = queryRunner.runPartitionIndexOrPartitionScanQueryOnGivenOwnedPartition(query, partitionId);
                future.addResult(partitionId, result);
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        }
    }

    private class ExecutionCallbackImpl implements BiConsumer<AtomicReferenceArray<Result>, Throwable> {
        private final QueryRunner queryRunner;
        private final Query query;

        ExecutionCallbackImpl(QueryRunner queryRunner, Query query) {
            this.queryRunner = queryRunner;
            this.query = query;
        }

        @Override
        public void accept(AtomicReferenceArray<Result> response, Throwable throwable) {
            if (throwable == null) {
                try {
                    Result combinedResult = queryRunner.populateEmptyResult(query, Collections.emptyList());
                    populateResult(response, combinedResult);
                    QueryOperation.this.sendResponse(combinedResult);
                } catch (Throwable e) {
                    QueryOperation.this.sendResponse(e);
                    throw rethrow(e);
                }
            } else {
                QueryOperation.this.sendResponse(throwable);
            }
        }

        private void populateResult(AtomicReferenceArray<Result> resultArray, Result combinedResult) {
            for (int k = 0; k < resultArray.length(); k++) {
                Result partitionResult = resultArray.get(k);
                if (partitionResult != null) {
                    combinedResult.combine(partitionResult);
                }
            }
        }
    }
}
