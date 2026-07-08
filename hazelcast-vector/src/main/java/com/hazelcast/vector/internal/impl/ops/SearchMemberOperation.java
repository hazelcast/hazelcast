/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.SelfResponseOperation;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.query.QueryResult;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.vector.internal.impl.Hints.MEMBER_LIMIT;
import static com.hazelcast.vector.internal.impl.service.VectorCollectionServiceImpl.VECTOR_QUERY_EXECUTOR;

/**
 * Performs search on given member and aggregates the results from member (1st stage of aggregation).
 * <p>
 * Executes on generic operation thread, but this involves only scheduling partition searches.
 * Searches and results merge are performed on appropriate executor.
 */
public class SearchMemberOperation extends AbstractNamedOperation
        implements IdentifiedDataSerializable, ReadonlyOperation, SelfResponseOperation {

    private VectorValues vectors;
    private SearchOptions searchOptions;
    private PartitionIdSet requestedPartitions;

    // execution state
    private transient ManagedExecutorService executor;
    private transient AtomicInteger finishedCounter;
    private transient QueryResult partitionResults;

    public SearchMemberOperation() {
    }

    public SearchMemberOperation(String vectorCollectionName, VectorValues vectors, SearchOptions searchOptions,
                                 PartitionIdSet requestedPartitions) {
        super(vectorCollectionName);
        this.vectors = vectors;
        this.searchOptions = searchOptions;
        this.requestedPartitions = requestedPartitions;
    }

    private void submitSearch(int partitionId) {
        // SearchMemberOperation does not execute on partition thread, which makes storage existence check harder.
        // SearchMemberOperation receives list of partition ids which can be different from current partitions
        // owned by the member if there were some migrations in the meantime.
        // There are 2 groups of problems: seeing incomplete data and wrongly assuming that the collection is empty.
        //
        // To avoid ABA problem (partition migrated out and back, but migration not yet complete,
        // but the storage could be already visible) and seeing incomplete data
        // we rely on the fact that partially migrated storage is never visible in getStorageOrNull.
        // In very unlikely case it is possible that search on the partition will be executed on member that
        // currently is not an owner, but if that happens:
        // - it can happen only when partition is during migration
        // - member executing the search already has all data
        // so the search result will be correct.
        //
        // We do not solve emptiness check problem here. If null storage is reported, search will be retried
        // on partition level were it is easier to know for sure.
        VectorCollectionService service = getService();
        VectorCollectionStorage maybeStorage = service.getStorageOrNull(getName(), partitionId);
        if (maybeStorage == null) {
            // Lack of storage means that either the member is not owner of the partition or the partition is empty.
            //
            // Assume we are not owner. That is more probable in real-world scenarios than a really empty partition.
            // This also does not require extra checks on partition thread (which would increase latency in common case).
            // If that ever becomes a problem with PartitionAware keys, then it is best to reconsider partitioning
            // of the data to avoid empty partitions in the first place.
            getLogger().fine("No longer owner of partition %d or partition is empty", partitionId);
            partitionDone(false);
            return;
        }

        executor.execute(() -> {
            try {
                var result = maybeStorage.search(vectors, searchOptions);
                // todo for hd
                // If the operation is executed locally, there won't be any serialization,
                // so we need to convert to the required type aka read from native memory manually.
                // use if (executedLocally()) {} or implement custom OperationResponseHandler
                partitionResults.addResult(partitionId, result);
            } catch (Throwable e) {
                // something unexpected happened. Log the error and the partition will be retried
                // individually by the coordinator.
                getLogger().warning("Search failed on partition " + partitionId, e);
            } finally {
                partitionDone(Thread.currentThread().isInterrupted());
            }
        });
    }

    private void partitionDone(boolean interrupted) {
        if (interrupted) {
            // If the thread was interrupted, it means that the instance is shutting down
            // or has already shut down if the timeout elapsed.
            // At this stage there are no network connections (server is already shut down)
            // so it does not make sense to prepare or send response.
            // Because server is shutdown before executors, some searches may try to send a response
            // between these event. This is not an error, generates clear warning log and the response
            // will not be sent. Callers on other members will be notified by MemberLeftException.
            //
            // This is unlikely with reasonable grace period (longer than search duration) for
            // executor service shutdown, but note that Vector index search does not use interruptible operations
            // and does not check for interruption status, so it has to execute fully
            // before it notices that the thread was interrupted.
            getLogger().info("Search finished after member termination, ignoring result");
        } else if (finishedCounter.decrementAndGet() == 0) {
            SearchMemberResult response = new SearchMemberResult(partitionResults.complete(),
                    partitionResults.getSourceIds());
            sendResponse(response);
        }
    }

    @Override
    public CallStatus call() throws Exception {
        executor = getNodeEngine().getExecutionService().getExecutor(VECTOR_QUERY_EXECUTOR);
        finishedCounter = new AtomicInteger(requestedPartitions.size());
        int limit = getMemberLimit(searchOptions);
        partitionResults = new QueryResult(getNodeEngine().getPartitionService().getPartitionCount(),
                requestedPartitions.size(), limit);

        return new Offload(this) {
            @Override
            public void start() {
                requestedPartitions.intIterator().forEachRemaining((IntConsumer) SearchMemberOperation.this::submitSearch);
            }
        };
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            // If the member is gone, it does not make sense to wait for it to return.
            // Even if it returns to the cluster it may have different set of partitions
            // due to rebalance process (unless the cluster is in FROZEN state).
            // Throw the exception to retry on partition level and avoid long waiting for the member.
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    private int getMemberLimit(SearchOptions searchOptions) {
        int resultLimit = searchOptions.getLimit();
        Integer maybeMemberLimit = MEMBER_LIMIT.get(searchOptions);
        if (maybeMemberLimit == null) {
            return resultLimit;
        }
        // unlike partitionLimit, invalid value of memberLimit is ignored.
        // Even if the validation failed here, the partitions would be retried individually.
        if (maybeMemberLimit < 0) {
            getLogger().warning("Member limit cannot be negative - ignored");
            return resultLimit;
        }
        if (maybeMemberLimit > resultLimit) {
            getLogger().warning("Number of neighbours requested from member "
                    + "is greater than requested result size - ignored");
            return resultLimit;
        }
        if (maybeMemberLimit * getNodeEngine().getClusterService().getSize(DATA_MEMBER_SELECTOR) < resultLimit) {
            // In case of member shutdown during query we might get false positives.
            // The warning is logged but the hint value is used anyway.
            getLogger().warning("Number of neighbours requested from member "
                    + "is not sufficient to generate full requested result");
        }
        return maybeMemberLimit;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return VectorCollectionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.SEARCH_MEMBER;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(vectors);
        out.writeObject(searchOptions);
        out.writeObject(requestedPartitions);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.vectors = in.readObject();
        this.searchOptions = in.readObject();
        this.requestedPartitions = in.readObject();
    }
}
