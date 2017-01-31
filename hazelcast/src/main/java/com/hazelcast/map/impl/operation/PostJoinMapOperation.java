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
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
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
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PostJoinMapOperation extends Operation implements IdentifiedDataSerializable {

    private List<MapIndexInfo> indexInfoList = new LinkedList<MapIndexInfo>();
    private List<InterceptorInfo> interceptorInfoList = new LinkedList<InterceptorInfo>();
    private List<AccumulatorInfo> infoList;

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void addMapIndex(MapContainer mapContainer) {
        final Indexes indexes = mapContainer.getIndexes();
        if (indexes.hasIndex()) {
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapContainer.getName());
            for (Index index : indexes.getIndexes()) {
                mapIndexInfo.addIndexInfo(index.getAttributeName(), index.isOrdered());
            }
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
        for (MapIndexInfo mapIndex : indexInfoList) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(mapIndex.mapName);
            final Indexes indexes = mapContainer.getIndexes();
            for (MapIndexInfo.IndexInfo indexInfo : mapIndex.lsIndexes) {
                indexes.addOrGetIndex(indexInfo.attributeName, indexInfo.ordered);
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
            publisherRegistry.getOrCreate(info.getCacheName());
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
        infoSupplier.putIfAbsent(info.getMapName(), info.getCacheName(), info);
    }

    public void setInfoList(List<AccumulatorInfo> infoList) {
        this.infoList = infoList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(indexInfoList.size());
        for (MapIndexInfo mapIndex : indexInfoList) {
            mapIndex.writeData(out);
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
        int indexesCount = in.readInt();
        for (int i = 0; i < indexesCount; i++) {
            MapIndexInfo mapIndexInfo = new MapIndexInfo();
            mapIndexInfo.readData(in);
            indexInfoList.add(mapIndexInfo);
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

    public static class MapIndexInfo implements IdentifiedDataSerializable {
        private String mapName;
        private List<MapIndexInfo.IndexInfo> lsIndexes = new LinkedList<MapIndexInfo.IndexInfo>();

        public MapIndexInfo(String mapName) {
            this.mapName = mapName;
        }

        public MapIndexInfo() {
        }

        public static class IndexInfo implements IdentifiedDataSerializable {
            private String attributeName;
            private boolean ordered;

            public IndexInfo() {
            }

            IndexInfo(String attributeName, boolean ordered) {
                this.attributeName = attributeName;
                this.ordered = ordered;
            }

            @Override
            public void writeData(ObjectDataOutput out) throws IOException {
                out.writeUTF(attributeName);
                out.writeBoolean(ordered);
            }

            @Override
            public void readData(ObjectDataInput in) throws IOException {
                attributeName = in.readUTF();
                ordered = in.readBoolean();
            }

            @Override
            public int getFactoryId() {
                return MapDataSerializerHook.F_ID;
            }

            @Override
            public int getId() {
                return MapDataSerializerHook.INDEX_INFO;
            }
        }

        public void addIndexInfo(String attributeName, boolean ordered) {
            lsIndexes.add(new MapIndexInfo.IndexInfo(attributeName, ordered));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(mapName);
            out.writeInt(lsIndexes.size());
            for (MapIndexInfo.IndexInfo indexInfo : lsIndexes) {
                indexInfo.writeData(out);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readUTF();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                MapIndexInfo.IndexInfo indexInfo = new MapIndexInfo.IndexInfo();
                indexInfo.readData(in);
                lsIndexes.add(indexInfo);
            }
        }

        @Override
        public int getFactoryId() {
            return MapDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return MapDataSerializerHook.MAP_INDEX_INFO;
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
