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

package com.hazelcast.connector;

import com.hazelcast.connector.map.Hz3MapAdapter;
import com.hazelcast.connector.map.AsyncMap;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * TODO documentation how to use
 */
public final class Hz3Enrichment {

    private Hz3Enrichment() {
    }

    /**
     * Create a service factory for enrichment using Hazelcast 3 remote map.
     * <p>
     * See the class javadoc for usage.
     */
    public static <K, V> ServiceFactory<Hz3MapAdapter, AsyncMap<K, V>> hz3MapServiceFactory(
            String mapName, String clientXML
    ) {
        return ServiceFactory.withCreateContextFn(context -> Hz3Util.createMapAdapter(clientXML))
                .withCreateServiceFn((context, hz3MapAdapter) -> {
                    AsyncMap<K, V> v = hz3MapAdapter.getMap(mapName);
                    return v;
                })
                .withDestroyContextFn(Hz3MapAdapter::shutdown);
    }

    /**
     * Create a service factory for enrichment using Hazelcast 3 remote replicated map.
     * <p>
     * See the class javadoc for usage.
     */
    public static <K, V> ServiceFactory<Hz3MapAdapter, Map<K, V>> hz3ReplicatedMapServiceFactory(
            String mapName, String clientXML
    ) {
        return ServiceFactory.withCreateContextFn(context -> Hz3Util.createMapAdapter(clientXML))
                .withCreateServiceFn((context, hz3MapAdapter) -> {
                    Map<K, V> v = hz3MapAdapter.getReplicatedMap(mapName);
                    return v;
                })
                .withDestroyContextFn(Hz3MapAdapter::shutdown);
    }

    /**
     * Helper function to convert simple lookupKeyFn and mapFn to mapAsyncFn required by
     * {@link GeneralStage#mapUsingServiceAsync(ServiceFactory, BiFunctionEx)}
     * <p>
     * See the class javadoc for usage.
     */
    public static <K, V, T, R> BiFunctionEx<? super AsyncMap<K, V>, ? super T, CompletableFuture<R>> mapUsingIMapAsync(
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (kvMap, t) -> kvMap.getAsync(lookupKeyFn.apply(t))
                .toCompletableFuture()
                .thenApply(e -> mapFn.apply(t, e));
    }

    /**
     * Helper function to convert simple lookupKeyFn and mapFn to mapAsyncFn required by
     * {@link GeneralStage#mapUsingServiceAsync(ServiceFactory, BiFunctionEx)}
     * <p>
     * See the class javadoc for usage.
     */
    public static <K, V, T, R> BiFunctionEx<? super Map<K, V>, ? super T, R> mapUsingIMap(
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (kvMap, t) -> mapFn.apply(t, kvMap.get(lookupKeyFn.apply(t)));
    }

}
