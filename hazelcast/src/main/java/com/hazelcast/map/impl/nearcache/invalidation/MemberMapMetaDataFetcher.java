/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataFetcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapGetInvalidationMetaDataOperation;
import com.hazelcast.map.impl.operation.MapGetInvalidationMetaDataOperation.MetaDataResponse;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * {@code MetaDataFetcher} for member side usage
 */
public class MemberMapMetaDataFetcher extends MetaDataFetcher {

    private final ClusterService clusterService;
    private final OperationService operationService;

    public MemberMapMetaDataFetcher(ClusterService clusterService, OperationService operationService, ILogger logger) {
        super(logger);
        this.clusterService = clusterService;
        this.operationService = operationService;
    }

    @Override
    protected void extractAndPopulateResult(InternalCompletableFuture future, ResultHolder resultHolder) throws Exception {
        MetaDataResponse response = (MetaDataResponse) future.get(ASYNC_RESULT_WAIT_TIMEOUT_MINUTES, MINUTES);
        resultHolder.populate(response.getPartitionUuidList().entrySet(), response.getNamePartitionSequenceList().entrySet());
    }

    @Override
    protected List<InternalCompletableFuture> scanMembers(List<String> names) {
        Collection<Member> members = clusterService.getMembers(DATA_MEMBER_SELECTOR);
        List<InternalCompletableFuture> futures = new ArrayList<InternalCompletableFuture>(members.size());
        for (Member member : members) {
            Operation operation = new MapGetInvalidationMetaDataOperation(names);
            Address address = member.getAddress();
            try {
                futures.add(operationService.invokeOnTarget(SERVICE_NAME, operation, address));
            } catch (Exception e) {
                if (logger.isWarningEnabled()) {
                    logger.warning("Cant fetch invalidation meta-data from address + " + address + " + [" + e.getMessage() + "]");
                }
            }
        }
        return futures;
    }
}
