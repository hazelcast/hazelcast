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

package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec.ResponseParameters;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec.decodeResponse;
import static com.hazelcast.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec.encodeRequest;
import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * {@code InvalidationMetaDataFetcher} for client side usage
 */
public class ClientMapInvalidationMetaDataFetcher extends InvalidationMetaDataFetcher {

    private final ClientClusterService clusterService;
    private final HazelcastClientInstanceImpl clientImpl;

    public ClientMapInvalidationMetaDataFetcher(ClientContext clientContext) {
        super(clientContext.getLoggingService().getLogger(ClientMapInvalidationMetaDataFetcher.class));
        this.clusterService = clientContext.getClusterService();
        this.clientImpl = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
    }

    @Override
    protected Collection<Member> getDataMembers() {
        return clusterService.getMembers(DATA_MEMBER_SELECTOR);
    }

    @Override
    protected InternalCompletableFuture fetchMetadataOf(Member member, List<String> names) {
        ClientMessage message = encodeRequest(names, member.getUuid());
        ClientInvocation invocation = new ClientInvocation(clientImpl, message, null, member.getUuid());
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
