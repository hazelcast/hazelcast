/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PostJoinMapOperation extends Operation implements IdentifiedDataSerializable {

    private List<MapIndexInfo> indexInfoList = new LinkedList<MapIndexInfo>();
    private List<InterceptorInfo> interceptorInfoList = new LinkedList<InterceptorInfo>();
    // RU_COMPAT_38
    private List<AccumulatorInfo> infoList;

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void addMapIndex(MapServiceContext mapServiceContext, MapContainer mapContainer) {
        // RU_COMPAT_38, it can be completely removed in 3.10+ master codebase
        if (mapContainer.isGlobalIndexEnabled()) {
            // GLOBAL-INDEX
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapContainer.getName());
            for (Index index : mapContainer.getIndexes().getIndexes()) {
                mapIndexInfo.addIndexInfo(index.getAttributeName(), index.isOrdered());
            }
            indexInfoList.add(mapIndexInfo);
        } else {
            // PARTITIONED-INDEX
            // in case of partitioned-index we gather all index infos in a set, since all partition should have the
            // same set of partitions. In theory it would be sufficient to gather data from only one partition, but
            // gathering data from all of them does no harm.
            Set<IndexInfo> indexInfos = new HashSet<IndexInfo>();
            for (PartitionContainer partitionContainer : mapServiceContext.getPartitionContainers()) {
                final Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
                if (indexes != null && indexes.hasIndex()) {
                    for (Index index : indexes.getIndexes()) {
                        indexInfos.add(new IndexInfo(index.getAttributeName(), index.isOrdered()));
                    }
                }
            }
            indexInfos.addAll(mapContainer.getPartitionIndexesToAdd());
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapContainer.getName());
            mapIndexInfo.addIndexInfos(indexInfos);
            indexInfoList.add(mapIndexInfo);
        }
    }

    public void addMapInterceptors(MapContainer mapContainer) {
        InterceptorRegistry interceptorRegistry = mapContainer.getInterceptorRegistry();
        List<MapInterceptor> interceptorList = interceptorRegistry.getInterceptors();
        Map<String, MapInterceptor> interceptorMap = interceptorRegistry.getId2InterceptorMap();
        Map<MapInterceptor, String> revMap = new HashMap<MapInterceptor, String>();
        for (Map.Entry<String, MapInterceptor> entry : interceptorMap.entrySet()) {
            revMap.put(entry.getValue(), entry.getKey());
        }
        InterceptorInfo interceptorInfo = new InterceptorInfo(mapContainer.getName());
        for (MapInterceptor interceptor : interceptorList) {
            interceptorInfo.addInterceptor(revMap.get(interceptor), interceptor);
        }
        interceptorInfoList.add(interceptorInfo);
    }

    public static class InterceptorInfo implements IdentifiedDataSerializable {

        private String mapName;
        private final List<Map.Entry<String, MapInterceptor>> interceptors = new LinkedList<Map.Entry<String, MapInterceptor>>();

        InterceptorInfo(String mapName) {
            this.mapName = mapName;
        }

        public InterceptorInfo() {
        }

        void addInterceptor(String id, MapInterceptor interceptor) {
            interceptors.add(new AbstractMap.SimpleImmutableEntry<String, MapInterceptor>(id, interceptor));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(mapName);
            out.writeInt(interceptors.size());
            for (Map.Entry<String, MapInterceptor> entry : interceptors) {
                out.writeUTF(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readUTF();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String id = in.readUTF();
                MapInterceptor interceptor = in.readObject();
                interceptors.add(new AbstractMap.SimpleImmutableEntry<String, MapInterceptor>(id, interceptor));
            }
        }

        @Override
        public int getFactoryId() {
            return MapDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return MapDataSerializerHook.INTERCEPTOR_INFO;
        }
    }

    @Override
    public void run() throws Exception {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        // RU_COMPAT_3_8
        // Can be removed in 3.10 master, since the indexes will be populated in the MapReplicationOperation
        for (MapIndexInfo mapIndex : indexInfoList) {
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapIndex.getMapName());
            for (IndexInfo indexInfo : mapIndex.getIndexInfos()) {
                if (mapContainer.isGlobalIndexEnabled()) {
                    // GLOBAL-INDEX
                    // we may add the index directly, since it's a global non-HD index, so adding it on a non-partition
                    // thread is a no issue.
                    Indexes indexes = mapContainer.getIndexes();
                    indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
                } else {
                    // PARTITIONED-INDEX
                    // Each partition thread is responsible for managing the lifecycle of a partitioned-index, thus
                    // we can't add an index here directly (also partitioned-index is used in HD only for now, and
                    // we are not allowed to touch HD memory on a non-partition thread).
                    // That is the reason why we gather all the dynamic post-join index infos for a map in the
                    // map-container. Later on they are picked up by the MapReplicationOperation which is spawned
                    // for each migrated partition, and the index is added by it for each partition.
                    mapContainer.addPartitionIndexToAdd(indexInfo);
                }
            }
        }
        for (InterceptorInfo interceptorInfo : interceptorInfoList) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(interceptorInfo.mapName);
            InterceptorRegistry interceptorRegistry = mapContainer.getInterceptorRegistry();
            Map<String, MapInterceptor> interceptorMap = interceptorRegistry.getId2InterceptorMap();
            List<Map.Entry<String, MapInterceptor>> entryList = interceptorInfo.interceptors;
            for (Map.Entry<String, MapInterceptor> entry : entryList) {
                if (!interceptorMap.containsKey(entry.getKey())) {
                    interceptorRegistry.register(entry.getKey(), entry.getValue());
                }
            }
        }
        createQueryCaches();
    }

    private void createQueryCaches() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();

        for (AccumulatorInfo info : infoList) {
            addAccumulatorInfo(queryCacheContext, info);

            PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrCreate(info.getMapName());
            publisherRegistry.getOrCreate(info.getCacheId());
            // marker listener.
            mapServiceContext.addLocalListenerAdapter(new ListenerAdapter<IMapEvent>() {
                @Override
                public void onEvent(IMapEvent event) {

                }
            }, info.getMapName());
        }
    }

    private void addAccumulatorInfo(QueryCacheContext context, AccumulatorInfo info) {
        PublisherContext publisherContext = context.getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.putIfAbsent(info.getMapName(), info.getCacheId(), info);
    }

    public void setInfoList(List<AccumulatorInfo> infoList) {
        this.infoList = infoList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        // RU_COMPAT_38
        // We don't need to send the index definition here in 3.9+ anymore
        if (out.getVersion().isUnknownOrLessOrEqual(Versions.V3_8)) {
            out.writeInt(indexInfoList.size());
            for (MapIndexInfo mapIndex : indexInfoList) {
                mapIndex.writeData(out);
            }
        }

        out.writeInt(interceptorInfoList.size());
        for (InterceptorInfo interceptorInfo : interceptorInfoList) {
            interceptorInfo.writeData(out);
        }
        int size = infoList.size();
        out.writeInt(size);
        for (AccumulatorInfo info : infoList) {
            out.writeObject(info);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // RU_COMPAT_38
        // We don't need to send the index definition here in 3.9+ anymore
        if (in.getVersion().isUnknownOrLessOrEqual(Versions.V3_8)) {
            int indexesCount = in.readInt();
            for (int i = 0; i < indexesCount; i++) {
                MapIndexInfo mapIndexInfo = new MapIndexInfo();
                mapIndexInfo.readData(in);
                indexInfoList.add(mapIndexInfo);
            }
        }

        int interceptorsCount = in.readInt();
        for (int i = 0; i < interceptorsCount; i++) {
            InterceptorInfo info = new InterceptorInfo();
            info.readData(in);
            interceptorInfoList.add(info);
        }
        int accumulatorsCount = in.readInt();
        if (accumulatorsCount < 1) {
            infoList = Collections.emptyList();
            return;
        }
        infoList = new ArrayList<AccumulatorInfo>(accumulatorsCount);
        for (int i = 0; i < accumulatorsCount; i++) {
            AccumulatorInfo info = in.readObject();
            infoList.add(info);
        }
    }


    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.POST_JOIN_MAP_OPERATION;
    }
}
