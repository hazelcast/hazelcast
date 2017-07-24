/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.util.CollectionUtil.toIntArray;

/**
 * Native handling only for RU compatibility purposes, can be deleted in 3.10 master
 * An old member may send a QueryOperation (and not HDQueryOperation) to an HD member.
 * In this case we want to handle it in the most efficient way.
 */
public class QueryOperation extends MapOperation implements ReadonlyOperation {

    private Query query;
    private Result result;

    public QueryOperation() {
    }

    public QueryOperation(Query query) {
        super(query.getMapName());
        this.query = query;
    }

    @Override
    public void run() throws Exception {
        QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(getName());
        if (isNativeInMemoryFormat()) {
            runAsyncPartitionThreadScanForNative(queryRunner);
        } else {
            result = queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        }

    }

    private void runAsyncPartitionThreadScanForNative(QueryRunner queryRunner) {
        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();
        ops.onStartAsyncOperation(this);
        runPartitionScanOnPartitionThreadsAsync(query, queryRunner);
    }

    void runPartitionScanOnPartitionThreadsAsync(final Query query, final QueryRunner queryRunner) {
        final List<Integer> initialPartitions = new ArrayList<Integer>(mapServiceContext.getOwnedPartitions());
        PartitionIteratingOperation opf = new PartitionIteratingOperation(
                new BinaryOperationFactory(new QueryPartitionOperation(query), getNodeEngine()), toIntArray(initialPartitions));

        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();
        ops.invokeOnTarget(MapService.SERVICE_NAME, opf, getNodeEngine().getThisAddress()).andThen(
                new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {
                        try {
                            Result modifiableResult = queryRunner.populateEmptyResult(query, initialPartitions);
                            populateResult((PartitionIteratingOperation.PartitionResponse) response, modifiableResult);
                            QueryOperation.this.sendResponse(modifiableResult);
                        } finally {
                            ops.onCompletionAsyncOperation(QueryOperation.this);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            QueryOperation.this.sendResponse(t);
                        } finally {
                            ops.onCompletionAsyncOperation(QueryOperation.this);
                        }
                    }
                });
    }

    private Result populateResult(PartitionIteratingOperation.PartitionResponse response, Result result) {
        // translate from Object[] (holding multiple Results) to a single Result object
        for (Object resultObject : response.getResults()) {
            if (resultObject instanceof Result) {
                Result partitionResult = (Result) resultObject;
                result.combine(partitionResult);
            }
            // otherwise the error will be handled anyway.
        }
        return result;
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
        if (isNativeInMemoryFormat()) {
            // This is required since if the returnsResponse() method returns false there won't be any response sent
            // to the invoking party - this means that the operation won't be retried if the exception is instanceof
            // HazelcastRetryableException
            sendResponse(e);
        } else {
            super.onExecutionFailure(e);
        }
    }

    @Override
    public boolean returnsResponse() {
        return !isNativeInMemoryFormat();
    }

    @Override
    public Object getResponse() {
        if (isNativeInMemoryFormat()) {
            return null;
        }
        return result;
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

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_OPERATION;
    }
}
