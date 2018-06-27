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

package com.hazelcast.client.cache.impl.nearcache.invalidation;


import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.ResponseParameters;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.Member;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.decodeResponse;
import static com.hazelcast.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.encodeRequest;
import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * {@code InvalidationMetaDataFetcher} for client side usage
 */
public class ClientCacheInvalidationMetaDataFetcher extends InvalidationMetaDataFetcher {

    private final ClientClusterService clusterService;
    private final HazelcastClientInstanceImpl clientImpl;

    public ClientCacheInvalidationMetaDataFetcher(ClientContext clientContext) {
        super(clientContext.getLoggingService().getLogger(ClientCacheInvalidationMetaDataFetcher.class));
        this.clusterService = clientContext.getClusterService();
        this.clientImpl = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
    }

    @Override
    protected Collection<Member> getDataMembers() {
        return clusterService.getMembers(DATA_MEMBER_SELECTOR);
    }

    @Override
    protected InternalCompletableFuture fetchMetadataOf(Address address, List<String> names) {
        ClientMessage message = encodeRequest(names, address);
        ClientInvocation invocation = new ClientInvocation(clientImpl, message, null, address);
        return invocation.invoke();
    }

    @Override
    protected void extractMemberMetadata(Member member,
                                         InternalCompletableFuture future,
                                         MetadataHolder metadataHolder) throws Exception {

        ClientMessage message = ((ClientMessage) future.get(ASYNC_RESULT_WAIT_TIMEOUT_MINUTES, MINUTES));
        ResponseParameters response = decodeResponse(message);

        metadataHolder.setMetadata(response.partitionUuidList, response.namePartitionSequenceList);
    }
}
