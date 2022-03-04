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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.journal.CacheEventJournalReadOperation;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Reads from the cache event journal in batches. You may specify the start sequence,
 * the minumum required number of items in the response, the maximum number of items
 * in the response, a predicate that the events should pass and a projection to
 * apply to the events in the journal.
 * If the event journal currently contains less events than the required minimum, the
 * call will wait until it has sufficient items.
 * The predicate, filter and projection may be {@code null} in which case all elements are returned
 * and no projection is applied.
 *
 * @param <K> cache key type
 * @param <V> cache value type
 * @param <T> the return type of the projection. It is equal to the journal event type
 *            if the projection is {@code null} or it is the identity projection
 * @see CacheEventJournalReadOperation
 * @since 3.9
 */
public class CacheEventJournalReadTask<K, V, T>
        extends AbstractCacheMessageTask<CacheEventJournalReadCodec.RequestParameters> {

    public CacheEventJournalReadTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        final Function<? super EventJournalCacheEvent<K, V>, T> projection
                = serializationService.toObject(parameters.projection);
        final Predicate<? super EventJournalCacheEvent<K, V>> predicate = serializationService.toObject(parameters.predicate);
        return new CacheEventJournalReadOperation<K, V, T>(parameters.name,
                parameters.startSequence, parameters.minSize, parameters.maxSize, predicate, projection);
    }

    @Override
    protected CacheEventJournalReadCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheEventJournalReadCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        // we are not deserializing the whole content, only the enclosing portable. The actual items remain un
        final ReadResultSetImpl resultSet = nodeEngine.getSerializationService().toObject(response);
        final List<Data> items = new ArrayList<Data>(resultSet.size());
        final long[] seqs = new long[resultSet.size()];
        final Data[] dataItems = resultSet.getDataItems();

        for (int k = 0; k < resultSet.size(); k++) {
            items.add(dataItems[k]);
            seqs[k] = resultSet.getSequence(k);
        }

        return CacheEventJournalReadCodec.encodeResponse(
                resultSet.readCount(), items, seqs, resultSet.getNextSequenceToReadFrom());
    }

    @Override
    public final String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "readFromEventJournal";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{
                parameters.startSequence, parameters.maxSize, getPartitionId(), parameters.predicate, parameters.projection, };
    }
}
