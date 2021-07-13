/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector.map.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.connector.map.Reader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.hazelcast.connector.map.impl.Hz3ImplUtil.toCompletableFuture;

public class MapReader {

    private static final int MAX_FETCH_SIZE = 16384;

    static class RemoteMapReader extends Reader<
            CompletableFuture<ClientMessage>,
            MapFetchEntriesCodec.ResponseParameters,
            Map.Entry<Data, Data>> {

        private final ClientMapProxy clientMapProxy;
        private final Function<Map.Entry<byte[], byte[]>, Object> toObject;

        RemoteMapReader(@Nonnull HazelcastInstance hzInstance,
                        @Nonnull String mapName,
                        Function<Map.Entry<byte[], byte[]>, Object> toObject
                        ) {
            super(mapName, r -> r.tableIndex, r -> r.entries);
            this.toObject = toObject;

            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(mapName);
        }

        @Nonnull
        @Override
        public CompletableFuture<ClientMessage> readBatch(int partitionId, int offset) {
            ClientMessage request = MapFetchEntriesCodec.encodeRequest(objectName, partitionId, offset, MAX_FETCH_SIZE);
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    objectName,
                    partitionId
            );
            ClientInvocationFuture future = clientInvocation.invoke();
            CompletableFuture<ClientMessage> result = toCompletableFuture(future);
            return result;
        }



        @Nonnull @Override
        public MapFetchEntriesCodec.ResponseParameters toBatchResult(@Nonnull CompletableFuture<ClientMessage> future)
                throws ExecutionException, InterruptedException {
            return MapFetchEntriesCodec.decodeResponse(future.get());
        }

        @Nullable
        @Override
        public Object toObject(@Nonnull Map.Entry<Data, Data> entry) {
            return toObject.apply(new AbstractMap.SimpleEntry<>(
                    entry.getKey().toByteArray(), entry.getValue().toByteArray()
            ));
        }
    }
}
