/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.iterator;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapEndEntryViewIterationCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapFetchEntryViewsCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * This class is responsible for iterating over the entries of a replicated map. It is not intended to be used when the
 * map is being mutated concurrently. In such case, the result may or may not include the newly added EntryViews.
 */
public class ClientReplicatedMapEntryViewIterator implements Iterator<ReplicatedMapEntryViewHolder> {
    private UUID currentCursorId;
    private List<ReplicatedMapEntryViewHolder> currentPage;
    // index is our current position in currentPage
    private int index;
    private boolean noMorePages;
    private final UUID iteratorId;
    private final int fetchSize;
    private final String mapName;
    private final int partitionId;
    private final ClientContext context;

    public ClientReplicatedMapEntryViewIterator(@Nonnull String mapName, int partitionId, @Nonnull UUID iteratorId,
                                                @Nonnull UUID currentCursorId, List<ReplicatedMapEntryViewHolder> initialPage,
                                                int fetchSize, ClientContext context) {
        this.mapName = mapName;
        this.partitionId = partitionId;
        this.iteratorId = iteratorId;
        this.currentCursorId = currentCursorId;
        this.currentPage = initialPage;
        this.fetchSize = fetchSize;
        this.index = 0;
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        return currentPage != null && index < currentPage.size();
    }

    @Override
    public ReplicatedMapEntryViewHolder next() {
        if (index >= currentPage.size()) {
            throw new NoSuchElementException();
        }
        ReplicatedMapEntryViewHolder nextItem = currentPage.get(index);
        index++;
        if (index >= currentPage.size()) {
            if (fetchNewPage()) {
                index = 0;
            }
        }
        return nextItem;
    }

    /**
     * Fetches a new page. Returns true if a new page is fetched.
     */
    private boolean fetchNewPage() {
        if (noMorePages) {
            return false;
        }
        ClientMessage message = ReplicatedMapFetchEntryViewsCodec.encodeRequest(this.mapName, this.currentCursorId, false,
                this.partitionId, fetchSize);
        ClientInvocation clientInvocation = new ClientInvocation((HazelcastClientInstanceImpl) context.getHazelcastInstance(),
                message, this.mapName, this.partitionId);
        try {
            ClientInvocationFuture f = clientInvocation.invoke();
            ReplicatedMapFetchEntryViewsCodec.ResponseParameters responseParameters
                    = ReplicatedMapFetchEntryViewsCodec.decodeResponse(f.get());
            this.currentPage = responseParameters.entryViews;
            if (this.currentPage.size() < fetchSize) {
                // we have read the last page, we can release the resources in the backend
                ClientMessage request = ReplicatedMapEndEntryViewIterationCodec.encodeRequest(this.mapName, this.iteratorId);
                ClientInvocation invocation = new ClientInvocation((HazelcastClientInstanceImpl) context.getHazelcastInstance(),
                        request, this.mapName, this.partitionId);
                invocation.invoke();
                noMorePages = true;
            }
            this.currentCursorId = responseParameters.cursorId;
            return true;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
