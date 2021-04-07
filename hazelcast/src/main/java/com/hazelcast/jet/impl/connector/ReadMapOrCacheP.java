/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.impl.CacheEntriesWithCursor;
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.operation.CacheFetchEntriesOperation;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.iterator.AbstractCursor;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.Partition;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.internal.iteration.IterationPointer.decodePointers;
import static com.hazelcast.internal.iteration.IterationPointer.encodePointers;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Private API, see methods in {@link SourceProcessors}.
 * <p>
 * The number of Hazelcast partitions should be configured to at least
 * {@code localParallelism * clusterSize}, otherwise some processors will
 * have no partitions assigned to them.
 */
public final class ReadMapOrCacheP<F extends CompletableFuture, B, R> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 16384;

    private final Reader<F, B, R> reader;
    private final int[] partitionIds;
    private final IterationPointer[][] readPointers;

    private F[] readFutures;

    // currently emitted batch, its iterating position and partitionId
    private List<R> currentBatch = Collections.emptyList();
    private int currentBatchPosition;
    private int currentPartitionIndex = -1;
    private int numCompletedPartitions;

    private Object pendingItem;

    private ReadMapOrCacheP(@Nonnull Reader<F, B, R> reader, @Nonnull int[] partitionIds) {
        this.reader = reader;
        this.partitionIds = partitionIds;

        readPointers = new IterationPointer[partitionIds.length][];
        Arrays.fill(readPointers, new IterationPointer[]{new IterationPointer(Integer.MAX_VALUE, -1)});
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        while (emitResultSet()) {
            if (!tryGetNextResultSet()) {
                return numCompletedPartitions == partitionIds.length;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void initialRead() {
        readFutures = (F[]) new CompletableFuture[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = reader.readBatch(partitionIds[i], readPointers[i]);
        }
    }

    private boolean emitResultSet() {
        if (pendingItem != null && !tryEmit(pendingItem)) {
            return false;
        }
        pendingItem = null;

        while (currentBatchPosition < currentBatch.size()) {
            Object item = reader.toObject(currentBatch.get(currentBatchPosition++));
            if (item == null) {
                // element was filtered out by the predicate (?)
                continue;
            }
            if (!tryEmit(item)) {
                pendingItem = item;
                return false;
            }
        }
        // we're done with the current batch
        return true;
    }

    private boolean tryGetNextResultSet() {
        while (currentBatch.size() == currentBatchPosition && ++currentPartitionIndex < partitionIds.length) {
            IterationPointer[] partitionPointers = readPointers[currentPartitionIndex];

            if (isDone(partitionPointers)) {  // partition is completed
                assert readFutures[currentPartitionIndex] == null : "future not null";
                continue;
            }

            F future = readFutures[currentPartitionIndex];
            if (!future.isDone()) {  // data for partition not yet available
                continue;
            }

            B result = toBatchResult(future);

            IterationPointer[] pointers = reader.toNextPointer(result);
            if (isDone(pointers)) {
                numCompletedPartitions++;
            } else {
                assert !currentBatch.isEmpty() : "empty but not terminal batch";
            }

            currentBatch = reader.toRecordSet(result);
            currentBatchPosition = 0;
            readPointers[currentPartitionIndex] = pointers;
            // make another read on the same partition
            readFutures[currentPartitionIndex] = !isDone(pointers)
                    ? reader.readBatch(partitionIds[currentPartitionIndex], pointers)
                    : null;
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            return false;
        }
        return true;
    }

    private boolean isDone(IterationPointer[] partitionPointers) {
        return partitionPointers[partitionPointers.length - 1].getIndex() < 0;
    }

    private B toBatchResult(F future) {
        B result;
        try {
            result = reader.toBatchResult(future);
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastSerializationException) {
                throw new JetException("Serialization error when reading the map: are the key, value, " +
                        "predicate and projection classes visible to IMDG? You need to use User Code " +
                        "Deployment, adding the classes to JetConfig isn't enough", e);
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
        return result;
    }

    static class LocalProcessorMetaSupplier<F extends CompletableFuture, B, R> implements ProcessorMetaSupplier {

        private static final long serialVersionUID = 1L;
        private final BiFunctionEx<HazelcastInstance, InternalSerializationService, Reader<F, B, R>> readerSupplier;

        private transient Map<Address, List<Integer>> addrToPartitions;

        LocalProcessorMetaSupplier(
                @Nonnull BiFunctionEx<HazelcastInstance, InternalSerializationService, Reader<F, B, R>> readerSupplier) {
            this.readerSupplier = readerSupplier;
        }

        @Override
        public void init(@Nonnull ProcessorMetaSupplier.Context context) {
            Set<Partition> partitions = context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions();
            addrToPartitions = partitions.stream()
                                         .collect(groupingBy(
                                                 partition -> partition.getOwner().getAddress(),
                                                 mapping(Partition::getPartitionId, toList()))
                                         );
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new LocalProcessorSupplier<>(readerSupplier, addrToPartitions.get(address));
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }
    }

    private static final class LocalProcessorSupplier<F extends CompletableFuture, B, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final BiFunction<HazelcastInstance, InternalSerializationService, Reader<F, B, R>> readerSupplier;
        private final List<Integer> memberPartitions;

        private transient HazelcastInstanceImpl hzInstance;
        private transient InternalSerializationService serializationService;

        private LocalProcessorSupplier(
                @Nonnull BiFunction<HazelcastInstance, InternalSerializationService, Reader<F, B, R>> readerSupplier,
                @Nonnull List<Integer> memberPartitions
        ) {
            this.readerSupplier = readerSupplier;
            this.memberPartitions = memberPartitions;
        }

        @Override
        public void init(@Nonnull Context context) {
            hzInstance = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
            serializationService = ((ProcSupplierCtx) context).serializationService();
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return distributeObjects(count, memberPartitions).values().stream()
                    .map(partitions -> partitions.stream().mapToInt(Integer::intValue).toArray())
                    .map(partitions ->
                            new ReadMapOrCacheP<>(readerSupplier.apply(hzInstance, serializationService), partitions))
                    .collect(toList());
        }
    }

    static class RemoteProcessorSupplier<F extends CompletableFuture, B, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final FunctionEx<HazelcastInstance, Reader<F, B, R>> readerSupplier;

        private transient HazelcastClientProxy client;
        private transient int totalParallelism;
        private transient int baseIndex;

        RemoteProcessorSupplier(
                @Nonnull String clientXml,
                @Nonnull FunctionEx<HazelcastInstance, Reader<F, B, R>> readerSupplier) {
            this.clientXml = clientXml;
            this.readerSupplier = readerSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = (HazelcastClientProxy) newHazelcastClient(asClientConfig(clientXml));
            totalParallelism = context.totalParallelism();
            baseIndex = context.memberIndex() * context.localParallelism();
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            int remotePartitionCount = client.client.getClientPartitionService().getPartitionCount();

            return IntStream.range(0, count)
                    .mapToObj(i -> {
                        int[] partitionIds = Util.roundRobinPart(remotePartitionCount, totalParallelism, baseIndex + i);
                        return new ReadMapOrCacheP<>(readerSupplier.apply(client), partitionIds);
                    })
                    .collect(Collectors.toList());
        }
    }

    /**
     * Stateless interface to read a map/cache.
     *
     * @param <F> type of the result future
     * @param <B> type of the batch object
     * @param <R> type of the record
     */
    abstract static class Reader<F extends CompletableFuture, B, R> {

        protected final String objectName;
        protected InternalSerializationService serializationService;

        private final FunctionEx<B, IterationPointer[]> toNextIterationPointerFn;
        private FunctionEx<B, List<R>> toRecordSetFn;

        Reader(@Nonnull String objectName,
               @Nonnull FunctionEx<B, IterationPointer[]> toNextIterationPointerFn,
               @Nonnull FunctionEx<B, List<R>> toRecordSetFn) {
            this.objectName = objectName;
            this.toNextIterationPointerFn = toNextIterationPointerFn;
            this.toRecordSetFn = toRecordSetFn;
        }

        @Nonnull
        abstract F readBatch(int partitionId, IterationPointer[] pointers);

        @Nonnull
        @SuppressWarnings("unchecked")
        B toBatchResult(@Nonnull F future) throws ExecutionException, InterruptedException {
            return (B) future.get();
        }

        final IterationPointer[] toNextPointer(@Nonnull B result) {
            return toNextIterationPointerFn.apply(result);
        }

        @Nonnull
        final List<R> toRecordSet(@Nonnull B result) {
            return toRecordSetFn.apply(result);
        }

        @Nullable
        abstract Object toObject(@Nonnull R record);

    }

    static class LocalCacheReader
            extends Reader<InternalCompletableFuture<CacheEntriesWithCursor>, CacheEntriesWithCursor, Entry<Data, Data>> {

        private final CacheProxy cacheProxy;

        LocalCacheReader(@Nonnull HazelcastInstance hzInstance,
                         @Nonnull InternalSerializationService serializationService,
                         @Nonnull String cacheName) {
            super(cacheName,
                    CacheEntriesWithCursor::getPointers,
                    CacheEntriesWithCursor::getEntries);

            this.cacheProxy = (CacheProxy) hzInstance.getCacheManager().getCache(cacheName);
            this.serializationService = serializationService;
        }

        @Nonnull @Override
        public InternalCompletableFuture<CacheEntriesWithCursor> readBatch(int partitionId, IterationPointer[] pointers) {
            Operation op = new CacheFetchEntriesOperation(cacheProxy.getPrefixedName(), pointers, MAX_FETCH_SIZE);
            //no access to CacheOperationProvider, have to be explicit
            OperationService operationService = cacheProxy.getOperationService();
            return operationService.invokeOnPartition(cacheProxy.getServiceName(), op, partitionId);
        }

        @Nullable @Override
        public Object toObject(@Nonnull Entry<Data, Data> dataEntry) {
            return new LazyMapEntry<>(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    static class RemoteCacheReader
            extends Reader<ClientInvocationFuture, CacheIterateEntriesCodec.ResponseParameters, Entry<Data, Data>> {

        private final ClientCacheProxy clientCacheProxy;

        RemoteCacheReader(@Nonnull HazelcastInstance hzInstance, @Nonnull String cacheName) {
            super(cacheName,
                    parameters -> decodePointers(parameters.iterationPointers),
                    parameters -> parameters.entries
            );
            this.clientCacheProxy = (ClientCacheProxy) hzInstance.getCacheManager().getCache(cacheName);
            this.serializationService = clientCacheProxy.getContext().getSerializationService();
        }

        @Nonnull @Override
        public ClientInvocationFuture readBatch(int partitionId, IterationPointer[] pointers) {
            String name = clientCacheProxy.getPrefixedName();
            ClientMessage request = CacheIterateEntriesCodec.encodeRequest(name, encodePointers(pointers), MAX_FETCH_SIZE);
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientCacheProxy.getContext()
                   .getHazelcastInstance();
            return new ClientInvocation(client, request, name, partitionId).invoke();
        }

        @Nonnull @Override
        public CacheIterateEntriesCodec.ResponseParameters toBatchResult(@Nonnull ClientInvocationFuture future)
                throws ExecutionException, InterruptedException {
            return CacheIterateEntriesCodec.decodeResponse(future.get());
        }

        @Nullable @Override
        public Object toObject(@Nonnull Entry<Data, Data> dataEntry) {
            return new LazyMapEntry<>(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    static class LocalMapReader
            extends Reader<InternalCompletableFuture<MapEntriesWithCursor>, MapEntriesWithCursor, Entry<Data, Data>> {

        private final MapProxyImpl mapProxyImpl;

        LocalMapReader(@Nonnull HazelcastInstance hzInstance,
                       @Nonnull InternalSerializationService serializationService,
                       @Nonnull String mapName) {
            super(mapName,
                    AbstractCursor::getIterationPointers,
                    AbstractCursor::getBatch);
            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(mapName);
            this.serializationService = serializationService;
        }

        @Nonnull @Override
        public InternalCompletableFuture<MapEntriesWithCursor> readBatch(int partitionId, IterationPointer[] pointers) {
            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            Operation op = operationProvider.createFetchEntriesOperation(objectName, pointers, MAX_FETCH_SIZE);
            return mapProxyImpl.getOperationService().invokeOnPartition(mapProxyImpl.getServiceName(), op, partitionId);
        }

        @Nullable @Override
        public Object toObject(@Nonnull Entry<Data, Data> dataEntry) {
            return new LazyMapEntry<>(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    static class LocalMapQueryReader
            extends Reader<InternalCompletableFuture<ResultSegment>, ResultSegment, QueryResultRow> {

        private final Predicate predicate;
        private final Projection projection;
        private final MapProxyImpl mapProxyImpl;

        LocalMapQueryReader(@Nonnull HazelcastInstance hzInstance,
                            @Nonnull InternalSerializationService serializationService,
                            @Nonnull String mapName,
                            @Nonnull Predicate predicate,
                            @Nonnull Projection projection) {
            super(mapName,
                    ResultSegment::getPointers,
                    segment -> ((QueryResult) segment.getResult()).getRows()
            );
            this.predicate = predicate;
            this.projection = projection;
            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(mapName);
            this.serializationService = serializationService;
        }

        @Nonnull @Override
        public InternalCompletableFuture<ResultSegment> readBatch(int partitionId, IterationPointer[] pointers) {
            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            MapOperation op = operationProvider.createFetchWithQueryOperation(
                    objectName,
                    pointers,
                    MAX_FETCH_SIZE,
                    Query.of()
                         .mapName(objectName)
                         .iterationType(IterationType.VALUE)
                         .predicate(predicate)
                         .projection(projection)
                         .build()
            );

            return mapProxyImpl.getOperationService().invokeOnPartition(mapProxyImpl.getServiceName(), op, partitionId);
        }

        @Nullable @Override
        public Object toObject(@Nonnull QueryResultRow record) {
            return serializationService.toObject(record.getValue());
        }
    }

    static class RemoteMapReader
            extends Reader<ClientInvocationFuture, MapFetchEntriesCodec.ResponseParameters, Entry<Data, Data>> {

        private final ClientMapProxy clientMapProxy;

        RemoteMapReader(@Nonnull HazelcastInstance hzInstance,
                        @Nonnull String mapName) {
            super(mapName, parameters -> decodePointers(parameters.iterationPointers), parameters -> parameters.entries);

            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(mapName);
            this.serializationService = clientMapProxy.getContext().getSerializationService();
        }

        @Nonnull @Override
        public ClientInvocationFuture readBatch(int partitionId, IterationPointer[] pointers) {
            ClientMessage request = MapFetchEntriesCodec.encodeRequest(
                    objectName, encodePointers(pointers), MAX_FETCH_SIZE
            );
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    objectName,
                    partitionId
            );
            return clientInvocation.invoke();
        }

        @Nonnull @Override
        public MapFetchEntriesCodec.ResponseParameters toBatchResult(@Nonnull ClientInvocationFuture future)
                throws ExecutionException, InterruptedException {
            return MapFetchEntriesCodec.decodeResponse(future.get());
        }

        @Nullable @Override
        public Entry<Data, Data> toObject(@Nonnull Entry<Data, Data> entry) {
            return new LazyMapEntry<>(entry.getKey(), entry.getValue(), serializationService);
        }
    }

    static class RemoteMapQueryReader
            extends Reader<ClientInvocationFuture, MapFetchWithQueryCodec.ResponseParameters, Data> {

        private final Predicate predicate;
        private final Projection projection;
        private final ClientMapProxy clientMapProxy;

        RemoteMapQueryReader(
                @Nonnull HazelcastInstance hzInstance,
                @Nonnull String mapName,
                @Nonnull Predicate predicate,
                @Nonnull Projection projection
        ) {
            super(mapName, r -> decodePointers(r.iterationPointers), r -> r.results);
            this.predicate = predicate;
            this.projection = projection;
            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(mapName);
            this.serializationService = clientMapProxy.getContext().getSerializationService();
        }

        @Nonnull @Override
        public ClientInvocationFuture readBatch(int partitionId, IterationPointer[] pointers) {
            ClientMessage request = MapFetchWithQueryCodec.encodeRequest(
                    objectName, encodePointers(pointers), MAX_FETCH_SIZE,
                    serializationService.toData(projection),
                    serializationService.toData(predicate)
            );
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    objectName,
                    partitionId
            );
            return clientInvocation.invoke();
        }

        @Nonnull @Override
        public MapFetchWithQueryCodec.ResponseParameters toBatchResult(@Nonnull ClientInvocationFuture future)
                throws ExecutionException, InterruptedException {
            return MapFetchWithQueryCodec.decodeResponse(future.get());
        }

        @Nullable @Override
        public Object toObject(@Nonnull Data data) {
            return serializationService.toObject(data);
        }
    }
}
