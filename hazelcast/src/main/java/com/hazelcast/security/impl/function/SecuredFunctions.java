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

package com.hazelcast.security.impl.function;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier.Context;
import com.hazelcast.jet.function.ToResultSetFunction;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadJdbcP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.jet.impl.connector.UpdateMapP;
import com.hazelcast.jet.impl.connector.UpdateMapWithEntryProcessorP;
import com.hazelcast.jet.impl.connector.WriteFileP;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.ReliableTopicPermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.topic.ITopic;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static com.hazelcast.security.PermissionsUtil.mapUpdatePermission;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUBLISH;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;
import static java.util.Collections.singletonList;

/**
 * Factory methods for functions which requires a permission to run.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
public final class SecuredFunctions {

    private SecuredFunctions() {
    }

    public static <K, V> FunctionEx<? super Context, IMap<K, V>> iMapFn(String name) {
        return new FunctionEx<Context, IMap<K, V>>() {
            @Override
            public IMap<K, V> applyEx(Context context) {
                return context.hazelcastInstance().getMap(name);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(new MapPermission(name, ACTION_CREATE, ACTION_READ));
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <K, V> FunctionEx<HazelcastInstance, EventJournalReader<EventJournalMapEvent<K, V>>>
    mapEventJournalReaderFn(String name) {
        return new FunctionEx<HazelcastInstance, EventJournalReader<EventJournalMapEvent<K, V>>>() {
            @Override
            public EventJournalReader<EventJournalMapEvent<K, V>> applyEx(HazelcastInstance instance) {
                return (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(name);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(new MapPermission(name, ACTION_CREATE, ACTION_READ));
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <K, V> FunctionEx<HazelcastInstance, EventJournalReader<EventJournalCacheEvent<K, V>>>
    cacheEventJournalReaderFn(String name) {
        return new FunctionEx<HazelcastInstance, EventJournalReader<EventJournalCacheEvent<K, V>>>() {
            @Override
            public EventJournalReader<EventJournalCacheEvent<K, V>> applyEx(HazelcastInstance instance) {
                return (EventJournalReader<EventJournalCacheEvent<K, V>>) instance.getCacheManager().getCache(name);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(new CachePermission(name, ACTION_CREATE, ACTION_READ));
            }
        };
    }

    public static <K, V> FunctionEx<? super Context, ReplicatedMap<K, V>> replicatedMapFn(String name) {
        return new FunctionEx<Context, ReplicatedMap<K, V>>() {
            @Override
            public ReplicatedMap<K, V> applyEx(Context context) {
                return context.hazelcastInstance().getReplicatedMap(name);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(new ReplicatedMapPermission(name, ACTION_CREATE, ACTION_READ));
            }
        };
    }

    public static SupplierEx<Processor> readListProcessorFn(String name, String clientXml) {
        return new SupplierEx<Processor>() {
            @Override
            public Processor getEx() {
                return new ReadIListP(name, clientXml);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(PermissionsUtil.listReadPermission(clientXml, name));
            }
        };
    }

    public static <E> FunctionEx<Processor.Context, ITopic<E>> reliableTopicFn(String name) {
        return new FunctionEx<Processor.Context, ITopic<E>>() {
            @Override
            public ITopic<E> applyEx(Processor.Context context) {
                return context.hazelcastInstance().getReliableTopic(name);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(new ReliableTopicPermission(name, ACTION_CREATE, ACTION_PUBLISH));
            }
        };
    }

    public static <S> BiFunctionEx<? super Processor.Context, Void, ? extends S> createServiceFn(
            FunctionEx<? super Processor.Context, ? extends S> createContextFn
    ) {
        return new BiFunctionEx<Processor.Context, Void, S>() {
            @Override
            public S applyEx(Processor.Context context, Void o) throws Exception {
                return createContextFn.applyEx(context);
            }

            @Override
            public List<Permission> permissions() {
                return createContextFn.permissions();
            }
        };
    }

    public static SupplierEx<Processor> streamSocketProcessorFn(String host, int port, String charset) {
        return new SupplierEx<Processor>() {
            @Override
            public Processor getEx() {
                return new StreamSocketP(host, port, Charset.forName(charset));
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.socket(host, port, ACTION_READ));
            }
        };
    }

    public static <T> FunctionEx<? super Path, Stream<T>> readFileFn(
            String directory,
            String charsetName,
            BiFunctionEx<? super String, ? super String, ? extends T> mapOutputFn
    ) {
        return new FunctionEx<Path, Stream<T>>() {
            @Override
            public Stream<T> applyEx(Path path) throws Exception {
                String fileName = path.getFileName().toString();
                return Files.lines(path, Charset.forName(charsetName))
                        .map(l -> mapOutputFn.apply(fileName, l));
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directory, ACTION_READ));
            }
        };
    }

    public static <T> FunctionEx<? super Path, ? extends Stream<T>> jsonReadFileFn(
            String directory,
            Class<T> type
    ) {
        return new FunctionEx<Path, Stream<T>>() {
            @Override
            public Stream<T> applyEx(Path path) throws Exception {
                return JsonUtil.beanSequenceFrom(path, type);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directory, ACTION_READ));
            }
        };
    }

    public static <T> FunctionEx<? super Path, ? extends Stream<Map<String, Object>>> jsonReadFileFn(
            String directory
    ) {
        return new FunctionEx<Path, Stream<Map<String, Object>>>() {
            @Override
            public Stream<Map<String, Object>> applyEx(Path path) throws Exception {
                return JsonUtil.mapSequenceFrom(path);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directory, ACTION_READ));
            }
        };
    }

    public static SupplierEx<Processor> streamFileProcessorFn(
            String watchedDirectory,
            String charset,
            String glob,
            boolean sharedFileSystem,
            BiFunctionEx<? super String, ? super String, ?> mapOutputFn
    ) {
        return new SupplierEx<Processor>() {
            @Override
            public Processor getEx() {
                return new StreamFilesP<>(watchedDirectory, Charset.forName(charset), glob,
                        sharedFileSystem, mapOutputFn);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(watchedDirectory, ACTION_READ));
            }
        };
    }

    public static <T> SupplierEx<Processor> readJdbcProcessorFn(
            String connectionUrl,
            SupplierEx<? extends Connection> newConnectionFn,
            ToResultSetFunction resultSetFn,
            FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return new SupplierEx<Processor>() {
            @Override
            public Processor getEx() {
                return new ReadJdbcP<>(newConnectionFn, resultSetFn, mapOutputFn);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.jdbc(connectionUrl, ACTION_READ));
            }
        };
    }

    public static FunctionEx<? super Processor.Context, ? extends BufferedWriter> createBufferedWriterFn(
            String host, int port, String charsetName
    ) {
        return new FunctionEx<Processor.Context, BufferedWriter>() {
            @Override
            public BufferedWriter applyEx(Processor.Context context) throws Exception {
                return new BufferedWriter(new OutputStreamWriter(new Socket(host, port).getOutputStream(), charsetName));
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.socket(host, port, ACTION_WRITE));
            }
        };
    }

    public static <T> SupplierEx<Processor> writeFileProcessorFn(
            String directoryName,
            FunctionEx<? super T, ? extends String> toStringFn,
            String charset,
            String datePattern,
            long maxFileSize,
            boolean exactlyOnce,
            LongSupplier clock
    ) {
        return new SupplierEx<Processor>() {
            @Override
            public Processor getEx() {
                return new WriteFileP<>(directoryName, toStringFn, charset, datePattern, maxFileSize, exactlyOnce, clock);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directoryName, ACTION_WRITE));
            }
        };
    }

    public static <T, K, V> FunctionEx<HazelcastInstance, Processor> updateMapProcessorFn(
            String name,
            String clientXml,
            FunctionEx<? super T, ? extends K> toKeyFn,
            BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        return new FunctionEx<HazelcastInstance, Processor>() {

            @Override
            public Processor applyEx(HazelcastInstance instance) {
                return new UpdateMapP<>(instance, name, toKeyFn, updateFn);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(mapUpdatePermission(clientXml, name));
            }
        };
    }

    public static <T, R, K, V> FunctionEx<HazelcastInstance, Processor> updateWithEntryProcessorFn(
            int maxParallelAsyncOps,
            String name,
            String clientXml,
            FunctionEx<? super T, ? extends K> toKeyFn,
            FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        return new FunctionEx<HazelcastInstance, Processor>() {
            @Override
            public Processor applyEx(HazelcastInstance instance) throws Exception {
                return new UpdateMapWithEntryProcessorP<>(instance, maxParallelAsyncOps, name,
                        toKeyFn, toEntryProcessorFn);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(mapUpdatePermission(clientXml, name));
            }
        };
    }
}
