/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.ToResultSetFunction;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.Util.cacheEventToEntry;
import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.processor.SourceProcessors.readCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamSocketP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sources. To start
 * building a pipeline, pass a source to {@link Pipeline#readFrom(BatchSource)}
 * and you will obtain the initial {@link BatchStage}. You can then
 * attach further stages to it.
 * <p>
 * The same pipeline may contain more than one source, each starting its
 * own branch. The branches may be merged with multiple-input transforms
 * such as co-group and hash-join.
 * <p>
 * The default local parallelism for sources in this class is 1 or 2, check the
 * documentation of individual methods.
 *
 * @since 3.0
 */
public final class Sources {

    private Sources() {
    }

    /**
     * Returns a bounded (batch) source constructed directly from the given
     * Core API processor meta-supplier.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    @Nonnull
    public static <T> BatchSource<T> batchFromProcessor(
            @Nonnull String sourceName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        checkSerializable(metaSupplier, "metaSupplier");
        return new BatchSourceTransform<>(sourceName, metaSupplier);
    }

    /**
     * Returns an unbounded (event stream) source that will use the supplied
     * function to create processor meta-suppliers as required by the Core API.
     * Jet will call the function you supply with an {@link EventTimePolicy}
     * and it must return a meta-supplier of processors that will act according
     * to the parameters in the policy and must emit the watermark items as the
     * policy specifies.
     * <p>
     * If you are implementing a custom source processor, be sure to check out
     * the {@link EventTimeMapper} class that will help you correctly implement
     * watermark emission.
     *  @param sourceName user-friendly source name
     * @param supportsNativeTimestamps true, if the processor is able to work
     * @param metaSupplierFn factory of processor meta-suppliers
     */
    @Nonnull
    public static <T> StreamSource<T> streamFromProcessorWithWatermarks(
            @Nonnull String sourceName,
            boolean supportsNativeTimestamps,
            @Nonnull Function<EventTimePolicy<? super T>, ProcessorMetaSupplier> metaSupplierFn
    ) {
        return new StreamSourceTransform<>(sourceName, metaSupplierFn, true, supportsNativeTimestamps);
    }

    /**
     * Returns an unbounded (event stream) source constructed directly from the given
     * Core API processor meta-supplier.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    @Nonnull
    public static <T> StreamSource<T> streamFromProcessor(
            @Nonnull String sourceName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        checkSerializable(metaSupplier, "metaSupplier");
        return new StreamSourceTransform<>(sourceName, w -> metaSupplier, false, false);
    }

    /**
     * Returns a source that fetches entries from a local Hazelcast {@code IMap}
     * with the specified name and emits them as {@code Map.Entry}. It leverages
     * data locality by making each of the underlying processors fetch only those
     * entries that are stored on the member where it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> map(@Nonnull String mapName) {
        return batchFromProcessor("mapSource(" + mapName + ')', readMapP(mapName));
    }

    /**
     * Returns a source that fetches entries from the given Hazelcast {@code
     * IMap} and emits them as {@code Map.Entry}. It leverages data locality
     * by making each of the underlying processors fetch only those entries
     * that are stored on the member where it is running.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> map(@Nonnull IMap<? extends K, ? extends V> map) {
        return map(map.getName());
    }

    /**
     * Returns a source that fetches entries from a local Hazelcast {@code
     * IMap} with the specified name. By supplying a {@code predicate} and
     * {@code projection} here instead of in separate {@code map/filter}
     * transforms you allow the source to apply these functions early, before
     * generating any output, with the potential of significantly reducing
     * data traffic. If your data is stored in the IMDG using the <a href=
     *     "http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link Projections#singleAttribute} and
     * {@link Projections#multiAttribute}) to create your projection instance and
     * using the {@link Predicates} factory or {@link PredicateBuilder}
     * to create the predicate. In this case Jet can test the predicate and
     * apply the projection without deserializing the whole object.
     * <p>
     * Due to the current limitations in the way Jet reads the map it can't use
     * any indexes on the map. It will always scan the map in full.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicate} and {@code projection} need
     * to be available on the cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class of
     * the objects stored in the map itself. If you cannot meet these
     * requirements, use {@link #map(String)} and add a subsequent
     * {@link GeneralStage#map map} or {@link GeneralStage#filter filter} stage.
     *
     * @param mapName the name of the map
     * @param predicate the predicate to filter the events. If you want to specify just the
     *                  projection, use {@link
     *                  Predicates#alwaysTrue()} as a pass-through
     *                  predicate
     * @param projection the projection to map the events. If the projection returns a {@code
     *                   null} for an item, that item will be filtered out. If you want to
     *                   specify just the predicate, use {@link Projections#identity()}.
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> map(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        return batchFromProcessor("mapSource(" + mapName + ')', readMapP(mapName, predicate, projection));
    }

    /**
     * Returns a source that fetches entries from the given Hazelcast {@code
     * IMap}. By supplying a {@code predicate} and {@code projection} here
     * instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
     * If your data is stored in the IMDG using the <a href=
     *   "http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link Projections#singleAttribute} and
     * {@link Projections#multiAttribute}) to create your projection instance
     * and using the {@link Predicates} factory or {@link PredicateBuilder}
     * to create the predicate. In this case Jet can test the predicate and
     * apply the projection without deserializing the whole object.
     * <p>
     * Due to the current limitations in the way Jet reads the map it can't use
     * any indexes on the map. It will always scan the map in full.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     * <p>
     * The classes implementing {@code predicate} and {@code projection} need
     * to be available on the cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the map itself. If you cannot meet these
     * requirements, use {@link #map(String)} and add a subsequent
     * {@link GeneralStage#map map} or {@link GeneralStage#filter filter} stage.
     *
     * @param map        the Hazelcast map to read data from
     * @param predicate  the predicate to filter the events. If you want to specify just the
     *                   projection, use {@link
     *                   Predicates#alwaysTrue()} as a pass-through
     *                   predicate
     * @param projection the projection to map the events. If the projection returns a {@code
     *                   null} for an item, that item will be filtered out. If you want to
     *                   specify just the predicate, use {@link Projections#identity()}.
     * @param <T>        type of emitted item
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> map(
            @Nonnull IMap<? extends K, ? extends V> map,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        return map(map.getName(), predicate, projection);
    }

    /**
     * Returns a source that will stream {@link EventJournalMapEvent}s of the
     * Hazelcast {@code IMap} with the specified name. By supplying a {@code
     * predicate} and {@code projection} here instead of in separate {@code
     * map/filter} transforms you allow the source to apply these functions
     * early, before generating any output, with the potential of significantly
     * reducing data traffic.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link EventJournalConfig
     * configure the event journal} for it. The journal has fixed capacity and
     * will drop events if it overflows.
     * <p>
     * The source saves the journal offsets to the snapshot. If the job
     * restarts, it starts emitting from the saved offsets with an exactly-once
     * guarantee (unless the journal has overflowed).
     * <p>
     * If you start a new job from an exported state, you can change the source
     * parameters as needed.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the map itself. If you cannot meet these
     * requirements, use {@link #mapJournal(String, JournalInitialPosition)} and
     * add a subsequent {@link GeneralStage#map map} or {@link GeneralStage#filter
     * filter} stage.
     *
     * @param <T>          type of emitted item
     * @param mapName      the name of the map
     * @param initialPos   describes which event to start receiving from
     * @param projectionFn the projection to map the events. If the projection returns a {@code
*                     null} for an item, that item will be filtered out. You may use {@link
*                     Util#mapEventToEntry()} to extract just the key and
*                     the new value.
     * @param predicateFn  the predicate to filter the events. If you want to specify just the
*                     projection, use {@link Util#mapPutEvents} to pass
*                     only {@link EntryEventType#ADDED ADDED} and
*                     {@link EntryEventType#UPDATED UPDATED} events.
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> mapJournal(
            @Nonnull String mapName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn
    ) {
        return streamFromProcessorWithWatermarks("mapJournalSource(" + mapName + ')',
                false, w -> streamMapP(mapName, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Returns a source that will stream {@link EventJournalMapEvent}s of the
     * given Hazelcast {@code IMap}. By supplying a {@code predicate} and {@code
     * projection} here instead of in separate {@code map/filter} transforms you
     * allow the source to apply these functions early, before generating any
     * output, with the potential of significantly reducing data traffic.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link EventJournalConfig
     * configure the event journal} for it. The journal has fixed capacity and
     * will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offsets to the snapshot. If the job
     * restarts, it starts emitting from the saved offsets with an exactly-once
     * guarantee (unless the journal has overflowed).
     * <p>
     * If you start a new job from an exported state, you can change the source
     * parameters as needed.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the map itself. If you cannot meet these
     * requirements, use {@link #mapJournal(String, JournalInitialPosition)}
     * and add a subsequent {@link GeneralStage#map map} or
     * {@link GeneralStage#filter filter} stage.
     *
     * <h4>Issue when "catching up"</h4>
     *
     * This processor does not coalesce watermarks from partitions. It reads
     * partitions one by one: it emits events from one partition and then from
     * another one in batches. This adds time disorder to events: it might emit
     * very recent event from partition1 while not yet emitting an old event
     * from partition2; and it generates watermarks based on this order. Even
     * if items in your partitions are ordered by timestamp, you can't use
     * allowed lag of 0. Most notably, the "catching up" happens after the job
     * is restarted, when events since the last snapshot are reprocessed in a
     * burst. In order to not lose any events, the lag should be configured to
     * at least {@code snapshotInterval + timeToRestart + normalEventLag}. The
     * reason for this behavior that the default partition count in the cluster
     * is pretty high and cannot by changed per object and for low-traffic maps
     * it takes long until all partitions see an event to allow emitting of a
     * coalesced watermark.
     *
     * @param <T>          type of emitted item
     * @param map          the map to read data from
     * @param initialPos   describes which event to start receiving from
     * @param projectionFn the projection to map the events. If the projection returns a {@code
*                     null} for an item, that item will be filtered out. You may use {@link
*                     Util#mapEventToEntry()} to extract just the key and
*                     the new value.
     * @param predicateFn  the predicate to filter the events. If you want to specify just the
*                     projection, use {@link Util#mapPutEvents} to pass
*                     only {@link EntryEventType#ADDED ADDED} and
*                     {@link EntryEventType#UPDATED UPDATED} events.
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> mapJournal(
            @Nonnull IMap<? extends K, ? extends V> map,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn
    ) {
        return mapJournal(map.getName(), initialPos, projectionFn, predicateFn);
    }

    /**
     * Convenience for {@link #mapJournal(String, JournalInitialPosition, FunctionEx, PredicateEx)}
     * which will pass only {@link EntryEventType#ADDED ADDED} and
     * {@link EntryEventType#UPDATED UPDATED} events and will project the
     * event's key and new value into a {@code Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> mapJournal(
            @Nonnull String mapName,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return mapJournal(mapName, initialPos, mapEventToEntry(), mapPutEvents());
    }

    /**
     * Convenience for {@link #mapJournal(IMap, JournalInitialPosition, FunctionEx, PredicateEx)}
     * which will pass only {@link EntryEventType#ADDED
     * ADDED} and {@link EntryEventType#UPDATED UPDATED}
     * events and will project the event's key and new value into a {@code
     * Map.Entry}.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> mapJournal(
            @Nonnull IMap<? extends K, ? extends V> map,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return mapJournal(map.getName(), initialPos, mapEventToEntry(), mapPutEvents());
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        return batchFromProcessor("remoteMapSource(" + mapName + ')',
                ProcessorMetaSupplier.of(readRemoteMapP(mapName, clientConfig)));
    }

    /**
     * Returns a source that fetches entries from a remote Hazelcast {@code
     * IMap} with the specified name in a remote cluster identified by the
     * supplied {@code ClientConfig}. By supplying a {@code predicate} and
     * {@code projection} here instead of in separate {@code map/filter}
     * transforms you allow the source to apply these functions early, before
     * generating any output, with the potential of significantly reducing
     * data traffic. If your data is stored in the IMDG using the <a href=
     *     "http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link Projections#singleAttribute} and {@link
     * Projections#multiAttribute}) to create your projection instance and
     * using the {@link Predicates} factory or
     * {@link PredicateBuilder PredicateBuilder} to create
     * the predicate. In this case Jet can test the predicate and apply the
     * projection without deserializing the whole object.
     * <p>
     * Due to the current limitations in the way Jet reads the map it can't use
     * any indexes on the map. It will always scan the map in full.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicate} and {@code projection} need
     * to be available on the remote cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the map itself. If you cannot meet these
     * conditions, use {@link #remoteMap(String, ClientConfig)} and add a
     * subsequent {@link GeneralStage#map map} or {@link GeneralStage#filter
     * filter} stage.
     *
     * @param mapName the name of the map
     * @param predicate the predicate to filter the events. If you want to specify just the
     *                  projection, use {@link Predicates#alwaysTrue()}
     *                  as a pass-through predicate
     * @param projection the projection to map the events. If the projection returns a {@code
     *                   null} for an item, that item will be filtered out. If you want to
     *                   specify just the predicate, use {@link Projections#identity()}.
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        return batchFromProcessor("remoteMapSource(" + mapName + ')',
                ProcessorMetaSupplier.of(readRemoteMapP(mapName, clientConfig, predicate, projection)));
    }

    /**
     * Returns a source that will stream the {@link EventJournalMapEvent}
     * events of the Hazelcast {@code IMap} with the specified name from a
     * remote cluster. By supplying a {@code predicate} and {@code projection}
     * here instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link EventJournalConfig
     * configure the event journal} for it. The journal has fixed capacity and
     * will drop events if it overflows.
     * <p>
     * The source saves the journal offsets to the snapshot. If the job
     * restarts, it starts emitting from the saved offsets with an exactly-once
     * guarantee (unless the journal has overflowed).
     * <p>
     * If you start a new job from an exported state, you can change the source
     * parameters as needed. If you connect to another cluster, keep in mind
     * that the same offsets will be used. To avoid this, give different
     * {@linkplain Stage#setName name} to this source.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the remote cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the map itself. If you cannot meet these
     * requirements, use {@link #remoteMapJournal(String, ClientConfig, JournalInitialPosition)}
     * and add a subsequent {@link GeneralStage#map map} or
     * {@link GeneralStage#filter filter} stage.
     *  @param <K> type of key
     * @param <V> type of value
     * @param <T> type of emitted item
     * @param mapName the name of the map
     * @param clientConfig configuration for the client to connect to the remote cluster
     * @param initialPos describes which event to start receiving from
     * @param projectionFn the projection to map the events. If the projection returns a {@code
*                     null} for an item, that item will be filtered out. You may use {@link
*                     Util#mapEventToEntry()} to extract just the key and
*                     the new value.
     * @param predicateFn the predicate to filter the events. You may use {@link
*                    Util#mapPutEvents} to pass only {@link
*                    EntryEventType#ADDED ADDED} and {@link EntryEventType#UPDATED UPDATED}
*                    events.
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn
    ) {
        return streamFromProcessorWithWatermarks("remoteMapJournalSource(" + mapName + ')',
                false, w -> streamRemoteMapP(mapName, clientConfig, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Convenience for {@link #remoteMapJournal(String, ClientConfig, JournalInitialPosition, FunctionEx, PredicateEx)}
     * which will pass only {@link EntryEventType#ADDED ADDED}
     * and {@link EntryEventType#UPDATED UPDATED} events and will
     * project the event's key and new value into a {@code Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return remoteMapJournal(mapName, clientConfig, initialPos, mapEventToEntry(), mapPutEvents());
    }

    /**
     * Returns a source that fetches entries from a Hazelcast {@code ICache}
     * with the given name and emits them as {@code Map.Entry}. It leverages
     * data locality by making each of the underlying processors fetch only
     * those entries that are stored on the member where it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> cache(@Nonnull String cacheName) {
        return batchFromProcessor("cacheSource(" + cacheName + ')', readCacheP(cacheName));
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of a Hazelcast {@code ICache} with the specified name. By
     * supplying a {@code predicate} and {@code projection} here instead of
     * in separate {@code map/filter} transforms you allow the source to apply
     * these functions early, before generating any output, with the potential
     * of significantly reducing data traffic.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code ICache} as a streaming source, you must {@link EventJournalConfig
     * configure the event journal} for it. The journal has fixed capacity and
     * will drop events if it overflows.
     * <p>
     * The source saves the journal offsets to the snapshot. If the job
     * restarts, it starts emitting from the saved offsets with an exactly-once
     * guarantee (unless the journal has overflowed).
     * <p>
     * If you start a new job from an exported state, you can change the source
     * parameters as needed.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the cache itself. If you cannot meet these
     * conditions, use {@link #cacheJournal(String, JournalInitialPosition)}
     * and add a subsequent {@link GeneralStage#map map} or
     * {@link GeneralStage#filter filter} stage.
     *
     * @param <T> type of emitted item
     * @param cacheName the name of the cache
     * @param initialPos describes which event to start receiving from
     * @param projectionFn the projection to map the events. If the projection returns a {@code
*                     null} for an item, that item will be filtered out. You may use {@link
*                     Util#cacheEventToEntry()} to extract just the key
*                     and the new value.
     * @param predicateFn the predicate to filter the events. You may use {@link
*                    Util#cachePutEvents()} to pass only {@link
*                    CacheEventType#CREATED CREATED} and {@link
*                    CacheEventType#UPDATED UPDATED} events.
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> cacheJournal(
            @Nonnull String cacheName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull FunctionEx<? super EventJournalCacheEvent<K, V>, ? extends T> projectionFn,
            @Nonnull PredicateEx<? super EventJournalCacheEvent<K, V>> predicateFn
    ) {
        return streamFromProcessorWithWatermarks("cacheJournalSource(" + cacheName + ')',
                false, w -> streamCacheP(cacheName, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Convenience for {@link #cacheJournal(String, JournalInitialPosition, FunctionEx, PredicateEx)}
     * which will pass only {@link CacheEventType#CREATED
     * CREATED} and {@link CacheEventType#UPDATED UPDATED}
     * events and will project the event's key and new value into a {@code
     * Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> cacheJournal(
            @Nonnull String cacheName,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return cacheJournal(cacheName, initialPos, cacheEventToEntry(), cachePutEvents());
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may miss
     * and/or duplicate some entries. If we detect a topology change, the job
     * will fail, but the detection is only on a best-effort basis - we might
     * still give incorrect results without reporting a failure. Concurrent
     * mutation is not detected at all.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> remoteCache(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        return batchFromProcessor("remoteCacheSource(" + cacheName + ')',
                ProcessorMetaSupplier.of(readRemoteCacheP(cacheName, clientConfig)));
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name from a
     * remote cluster. By supplying a {@code predicate} and {@code projection}
     * here instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * To use an {@code ICache} as a streaming source, you must {@link EventJournalConfig
     * configure the event journal} for it. The journal has fixed capacity and
     * will drop events if it overflows.
     * <p>
     * The source saves the journal offsets to the snapshot. If the job
     * restarts, it starts emitting from the saved offsets with an exactly-once
     * guarantee (unless the journal has overflowed).
     * <p>
     * If you start a new job from an exported state, you can change the source
     * parameters as needed. If you connect to another cluster, keep in mind
     * that the same offsets will be used. To avoid this, give different
     * {@linkplain Stage#setName name} to this source.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * the job classpath in {@link JobConfig}. The same is true for the class
     * of the objects stored in the cache itself. If you cannot meet these
     * conditions, use {@link #remoteCacheJournal(String, ClientConfig, JournalInitialPosition)}
     * and add a subsequent {@link GeneralStage#map map} or
     * {@link GeneralStage#filter filter} stage.
     *
     * @param <T> type of emitted item
     * @param cacheName the name of the cache
     * @param clientConfig configuration for the client to connect to the remote cluster
     * @param initialPos describes which event to start receiving from
     * @param projectionFn the projection to map the events. If the projection returns a {@code
*                     null} for an item, that item will be filtered out. You may use {@link
*                     Util#cacheEventToEntry()} to extract just the key
*                     and the new value.
     * @param predicateFn the predicate to filter the events. You may use {@link
*                    Util#cachePutEvents()} to pass only {@link
*                    CacheEventType#CREATED CREATED} and {@link
*                    CacheEventType#UPDATED UPDATED} events.
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> remoteCacheJournal(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull FunctionEx<? super EventJournalCacheEvent<K, V>, ? extends T> projectionFn,
            @Nonnull PredicateEx<? super EventJournalCacheEvent<K, V>> predicateFn
    ) {
        return streamFromProcessorWithWatermarks("remoteCacheJournalSource(" + cacheName + ')',
                false, w -> streamRemoteCacheP(cacheName, clientConfig, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Convenience for {@link #remoteCacheJournal(String, ClientConfig, JournalInitialPosition, FunctionEx, PredicateEx)}
     * which will pass only
     * {@link CacheEventType#CREATED CREATED}
     * and {@link CacheEventType#UPDATED UPDATED}
     * events and will project the event's key and new value
     * into a {@code Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> remoteCacheJournal(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return remoteCacheJournal(cacheName, clientConfig, initialPos, cacheEventToEntry(), cachePutEvents());
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList}. All elements are emitted on a single member &mdash; the one
     * where the entire list is stored by the IMDG.
     * <p>
     * If the {@code IList} is modified while being read, the source may miss
     * and/or duplicate some entries.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * One instance of this processor runs only on the member that owns the
     * list.
     */
    @Nonnull
    public static <T> BatchSource<T> list(@Nonnull String listName) {
        return batchFromProcessor("listSource(" + listName + ')', readListP(listName));
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList}. All elements are emitted on a single member &mdash; the one
     * where the entire list is stored by the IMDG.
     * <p>
     * If the {@code IList} is modified while being read, the source may miss
     * and/or duplicate some entries.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the list you
     * supply and acquires a list with that name on the local cluster. If you
     * supply a list instance from another cluster, no error will be thrown to
     * indicate this.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * One instance of this processor runs only on the member that owns the
     * list.
     */
    @Nonnull
    public static <T> BatchSource<T> list(@Nonnull IList<? extends T> list) {
        return list(list.getName());
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList} in a remote cluster identified by the supplied {@code
     * ClientConfig}. All elements are emitted on a single member.
     * <p>
     * If the {@code IList} is modified while being read, the source may miss
     * and/or duplicate some entries.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Only 1 instance of this processor runs in the cluster.
     */
    @Nonnull
    public static <T> BatchSource<T> remoteList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return batchFromProcessor("remoteListSource(" + listName + ')', readRemoteListP(listName, clientConfig));
    }

    /**
     * Returns a source which connects to the specified socket and emits lines
     * of text received from it. It decodes the text using the supplied {@code
     * charset}.
     * <p>
     * Each underlying processor opens its own TCP connection, so there will be
     * {@code clusterSize * localParallelism} open connections to the server.
     * <p>
     * The source completes when the server closes the socket. It never attempts
     * to reconnect. Any {@code IOException} will cause the job to fail.
     * <p>
     * The source does not save any state to snapshot. On job restart, it will
     * emit whichever items the server sends. The implementation uses
     * non-blocking API, the processor is cooperative.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static StreamSource<String> socket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return streamFromProcessor(
                "socketSource(" + host + ':' + port + ')', streamSocketP(host, port, charset)
        );
    }

    /**
     * Convenience for {@link #socket socket(host, port, charset)} with
     * UTF-8 as the charset.
     *
     * @param host the hostname to connect to
     * @param port the port to connect to
     */
    @Nonnull
    public static StreamSource<String> socket(@Nonnull String host, int port) {
        return socket(host, port, UTF_8);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom source to read files for the Pipeline API. The source reads
     * lines from files in a directory (but not its subdirectories). Using this
     * builder you can build {@linkplain FileSourceBuilder#build() batching} or
     * {@linkplain FileSourceBuilder#buildWatcher() streaming} reader.
     */
    @Nonnull
    public static FileSourceBuilder filesBuilder(@Nonnull String directory) {
        return new FileSourceBuilder(directory);
    }

    /**
     * A source to read all files in a directory in a batch way.
     * <p>
     * This method is a shortcut for: <pre>{@code
     *   filesBuilder(directory)
     *      .charset(UTF_8)
     *      .glob(GLOB_WILDCARD)
     *      .sharedFileSystem(false)
     *      .mapToOutputFn((fileName, line) -> line)
     *      .build()
     * }</pre>
     * <p>
     * If files are appended to while being read, the addition might or might
     * not be emitted or part of a line can be emitted. If files are modified
     * in more complex ways, the behavior is undefined.
     *
     * See {@link #filesBuilder(String)}.
     */
    @Nonnull
    public static BatchSource<String> files(@Nonnull String directory) {
        return filesBuilder(directory).build();
    }

    /**
     * A source to stream lines added to files in a directory. This is a
     * streaming source, it will watch directory and emit lines as they are
     * appended to files in that directory.
     * <p>
     * This method is a shortcut for: <pre>{@code
     *   filesBuilder(directory)
     *      .charset(UTF_8)
     *      .glob(GLOB_WILDCARD)
     *      .sharedFileSystem(false)
     *      .mapToOutputFn((fileName, line) -> line)
     *      .buildWatcher()
     * }</pre>
     *
     * <h3>Appending lines using an text editor</h3>
     * If you're testing this source, you might think of using a text editor to
     * append the lines. However, it might not work as expected because some
     * editors write to a temp file and then rename it or append extra newline
     * character at the end which gets overwritten if more text is added in the
     * editor. Best way to append is to use {@code echo text >> yourfile}.
     *
     * See {@link #filesBuilder(String)}.
     */
    @Nonnull
    public static StreamSource<String> fileWatcher(@Nonnull String watchedDirectory) {
        return filesBuilder(watchedDirectory).buildWatcher();
    }

    /**
     * Convenience for {@link #jmsQueueBuilder(SupplierEx)}. This
     * version creates a connection without any authentication parameters and
     * uses non-transacted sessions with {@code Session.AUTO_ACKNOWLEDGE} mode.
     * JMS {@link Message} objects are emitted to downstream.
     * <p>
     * <b>Note:</b> {@link javax.jms.Message} might not be serializable. In
     * that case you can use {@linkplain #jmsQueueBuilder(SupplierEx)
     * the builder} and add a projection.
     *
     * @param factorySupplier supplier to obtain JMS connection factory
     * @param name            the name of the queue
     */
    @Nonnull
    public static StreamSource<Message> jmsQueue(
            @Nonnull SupplierEx<? extends ConnectionFactory> factorySupplier,
            @Nonnull String name
    ) {
        return jmsQueueBuilder(factorySupplier)
                .destinationName(name)
                .build();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom JMS {@link StreamSource} for the Pipeline API. See javadoc on
     * {@link JmsSourceBuilder} methods for more details.
     * <p>
     * This source uses the {@link Message#getJMSTimestamp() JMS' message
     * timestamp} as the native timestamp, if {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) enabled}.
     * <p>
     * The source does not save any state to snapshot. The source starts
     * emitting items where it left from.
     * <p>
     * IO failures should be handled by the JMS provider. If any JMS operation
     * throws an exception, the job will fail. Most of the providers offer a
     * configuration parameter to enable auto-reconnection, refer to provider
     * documentation for details.
     * <p>
     * Default local parallelism for this processor is 4 (or less if less CPUs
     * are available).
     */
    @Nonnull
    public static JmsSourceBuilder jmsQueueBuilder(SupplierEx<? extends ConnectionFactory> factorySupplier) {
        return new JmsSourceBuilder(factorySupplier, false);
    }

    /**
     * Convenience for {@link #jmsTopicBuilder(SupplierEx)}. This
     * version creates a connection without any authentication parameters and
     * uses non-transacted sessions with {@code Session.AUTO_ACKNOWLEDGE} mode.
     * JMS {@link Message} objects are emitted to downstream.
     * <p>
     * <b>Note:</b> {@link javax.jms.Message} might not be serializable. In
     * that case you can use {@linkplain #jmsTopicBuilder(SupplierEx)
     * the builder} and add a projection.
     *
     * @param factorySupplier supplier to obtain JMS connection factory
     * @param name            the name of the topic
     */
    @Nonnull
    public static StreamSource<Message> jmsTopic(
            @Nonnull SupplierEx<? extends ConnectionFactory> factorySupplier,
            @Nonnull String name
    ) {
        return jmsTopicBuilder(factorySupplier)
                .destinationName(name)
                .build();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom JMS {@link StreamSource} for the Pipeline API. See javadoc on
     * {@link JmsSourceBuilder} methods for more details.
     * <p>
     * Topic is a non-distributed source: if messages are consumed by multiple
     * consumers, all of them will get the same messages. Therefore the source
     * operates on a single member and with local parallelism of 1. Setting
     * local parallelism to a value other than 1 causes an {@code
     * IllegalArgumentException}.
     * <p>
     * This source uses the {@link Message#getJMSTimestamp() JMS' message
     * timestamp} as the native timestamp, if {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) enabled}.
     * <p>
     * The source does not save any state to snapshot. Behavior of job restart
     * changes according to the consumer. If it is a durable consumer and a
     * unique client identifier is set for the connection then JMS provider
     * persists items during restart and the source starts where it left from.
     * If the consumer is non-durable then source emits the items published
     * after the restart.
     * <p>
     * IO failures should be handled by the JMS provider. If any JMS operation
     * throws an exception, the job will fail. Most of the providers offer a
     * configuration parameter to enable auto-reconnection, refer to provider
     * documentation for details.
     */
    @Nonnull
    public static JmsSourceBuilder jmsTopicBuilder(SupplierEx<? extends ConnectionFactory> factorySupplier) {
        return new JmsSourceBuilder(factorySupplier, true);
    }

    /**
     * Returns a source which connects to the specified database using the given
     * {@code newConnectionFn}, queries the database and creates a result set
     * using the the given {@code resultSetFn}. It creates output objects from the
     * {@link ResultSet} using given {@code mapOutputFn} and emits them to
     * downstream.
     * <p>
     * {@code resultSetFn} gets the created connection, total parallelism (local
     * parallelism * member count) and global processor index as arguments and
     * produces a result set. The parallelism and processor index arguments
     * should be used to fetch a part of the whole result set specific to the
     * processor. If the table itself isn't partitioned by the same key, then
     * running multiple queries might not really be faster than using the
     * {@linkplain #jdbc(String, String, FunctionEx) simpler
     * version} of this method, do your own testing.
     * <p>
     * {@code createOutputFn} gets the {@link ResultSet} and creates desired
     * output object. The function is called for each row of the result set,
     * user should not call {@link ResultSet#next()} or any other
     * cursor-navigating functions.
     * <p>
     * Example: <pre>{@code
     *     p.readFrom(Sources.jdbc(
     *         () -> DriverManager.getConnection(DB_CONNECTION_URL),
     *         (con, parallelism, index) -> {
     *              PreparedStatement stmt = con.prepareStatement("SELECT * FROM TABLE WHERE MOD(id, ?) = ?)");
     *              stmt.setInt(1, parallelism);
     *              stmt.setInt(2, index);
     *              return stmt.executeQuery();
     *         },
     *         resultSet -> new Person(resultSet.getInt(1), resultSet.getString(2))))
     * }</pre>
     * <p>
     * If the underlying table is modified while being read, the source may
     * miss and/or duplicate some entries, because multiple queries for parts
     * of the data on multiple members will be executed.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Any {@code SQLException} will cause the job to fail.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * @param newConnectionFn creates the connection
     * @param resultSetFn creates a {@link ResultSet} using the connection,
     *                    total parallelism and index
     * @param createOutputFn creates output objects from {@link ResultSet}
     * @param <T> type of output objects
     */
    public static <T> BatchSource<T> jdbc(
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> createOutputFn
    ) {
        return batchFromProcessor("jdbcSource",
                SourceProcessors.readJdbcP(newConnectionFn, resultSetFn, createOutputFn));
    }

    /**
     * Convenience for {@link Sources#jdbc(SupplierEx,
     * ToResultSetFunction, FunctionEx)}.
     * A non-distributed, single-worker source which fetches the whole resultSet
     * with a single query on single member.
     * <p>
     * This method executes exactly one query in the target database. If the
     * underlying table is modified while being read, the behavior depends on
     * the configured transaction isolation level in the target database. Refer
     * to the documentation for the target database system.
     * <p>
     * Example: <pre>{@code
     *     p.readFrom(Sources.jdbc(
     *         DB_CONNECTION_URL,
     *         "select ID, NAME from PERSON",
     *         resultSet -> new Person(resultSet.getInt(1), resultSet.getString(2))))
     * }</pre>
     */
    public static <T> BatchSource<T> jdbc(
            @Nonnull String connectionURL,
            @Nonnull String query,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> createOutputFn
    ) {
        return batchFromProcessor("jdbcSource",
                SourceProcessors.readJdbcP(connectionURL, query, createOutputFn));
    }
}
