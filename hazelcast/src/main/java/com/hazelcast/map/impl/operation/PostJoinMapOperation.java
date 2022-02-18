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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.IMapEvent;
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
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.TargetAware;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class PostJoinMapOperation extends Operation implements IdentifiedDataSerializable, TargetAware {

    private List<InterceptorInfo> interceptorInfoList = new LinkedList<>();
    private List<AccumulatorInfo> infoList;

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void addMapInterceptors(MapContainer mapContainer) {
        InterceptorRegistry interceptorRegistry = mapContainer.getInterceptorRegistry();
        List<MapInterceptor> interceptorList = interceptorRegistry.getInterceptors();
        Map<String, MapInterceptor> interceptorMap = interceptorRegistry.getId2InterceptorMap();
        Map<MapInterceptor, String> revMap = createHashMap(interceptorMap.size());
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
        private final List<Map.Entry<String, MapInterceptor>> interceptors = new LinkedList<>();

        InterceptorInfo(String mapName) {
            this.mapName = mapName;
        }

        public InterceptorInfo() {
        }

        void addInterceptor(String id, MapInterceptor interceptor) {
            interceptors.add(new AbstractMap.SimpleImmutableEntry<>(id, interceptor));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeInt(interceptors.size());
            for (Map.Entry<String, MapInterceptor> entry : interceptors) {
                out.writeString(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String id = in.readString();
                MapInterceptor interceptor = in.readObject();
                interceptors.add(new AbstractMap.SimpleImmutableEntry<>(id, interceptor));
            }
        }

        @Override
        public int getFactoryId() {
            return MapDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return MapDataSerializerHook.INTERCEPTOR_INFO;
        }
    }

    @Override
    public void run() throws Exception {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

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
            mapServiceContext.addLocalListenerAdapter((ListenerAdapter<IMapEvent>) event -> {

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

        out.writeInt(interceptorInfoList.size());
        for (InterceptorInfo interceptorInfo : interceptorInfoList) {
            out.writeObject(interceptorInfo);
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

        int interceptorsCount = in.readInt();
        for (int i = 0; i < interceptorsCount; i++) {
            interceptorInfoList.add(in.readObject());
        }
        int accumulatorsCount = in.readInt();
        if (accumulatorsCount < 1) {
            infoList = Collections.emptyList();
            return;
        }
        infoList = new ArrayList<>(accumulatorsCount);
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
    public int getClassId() {
        return MapDataSerializerHook.POST_JOIN_MAP_OPERATION;
    }

    @Override
    public void setTarget(Address address) {
    }
}
