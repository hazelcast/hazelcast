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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.SelfResponseOperation;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.SearchResultsImpl;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage;

import java.io.IOException;

import static com.hazelcast.vector.internal.impl.service.VectorCollectionServiceImpl.VECTOR_QUERY_EXECUTOR;

public class SearchOperation extends AbstractNamedOperation
        implements IdentifiedDataSerializable, ReadonlyOperation, SelfResponseOperation {

    private VectorValues vectors;
    private SearchOptions searchOptions;
    private transient SearchResults<Data, Data> result;

    public SearchOperation() {
    }

    public SearchOperation(String vectorCollectionName, VectorValues vectors, SearchOptions searchOptions) {
        super(vectorCollectionName);
        this.vectors = vectors;
        this.searchOptions = searchOptions;
    }

    @Override
    public CallStatus call() {
        VectorCollectionService service = getService();
        VectorCollectionStorage maybeStorage = service.getStorageOrNull(getName(), getPartitionId());
        if (maybeStorage != null) {
            return new Offload(this) {
                @Override
                public void start() {
                    var executor = getNodeEngine().getExecutionService().getExecutor(VECTOR_QUERY_EXECUTOR);
                    executor.execute(() -> {
                        try {
                            var result = maybeStorage.search(vectors, searchOptions);
                            // todo for hd
                            // If the operation is executed locally, there won't be any serialization,
                            // so we need to convert to the required type aka read from native memory manually.
                            // use if (executedLocally()) {} or implement custom OperationResponseHandler
                            sendResponse(result);
                        } catch (Throwable e) {
                            sendResponse(e);
                        }
                    });
                }
            };
        } else {
            // the check executed on partition thread, so if we are here it means that:
            // - we are owner of the partition (operation was allowed to execute)
            // - there really is no storage (empty partition for given collection)
            // That is why we know that this is a correct result.
            result = SearchResultsImpl.EMPTY;

            // done without offload
            // the response will be sent even though returnsResponse() returns false
            return CallStatus.RESPONSE;
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return result;
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
        return VectorCollectionSerializerHook.SEARCH;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(vectors);
        out.writeObject(searchOptions);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.vectors = in.readObject();
        this.searchOptions = in.readObject();
    }
}
