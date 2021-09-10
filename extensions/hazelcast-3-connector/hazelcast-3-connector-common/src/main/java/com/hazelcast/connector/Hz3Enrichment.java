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
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Provides a way to perform enrichment using a Map or ReplicatedMap from
 * Hazelcast 3 cluster.
 * <p>
 * The usage is similar to
 * {@link GeneralStage#mapUsingIMap(String, FunctionEx, BiFunctionEx)} and
 * other similarly named methods. The difference is that instead of providing
 * a name of the map, a {@link ServiceFactory} to Hazelcast 3 Map is used.
 * This class provides utility methods to create and use this service factory.
 * <p>
 * Because of incompatible APIs the configuration is passed as an XML
 * document in a string - note that the XML configuration must conform
 * to 3.x schema - see <a href="https://www.hazelcast.com/schema/config/">
 * https://www.hazelcast.com/schema/config/</a>.
 * <p>
 * Usage:
 * <p>
 * First you need to obtain a ServiceFactory for a Hazelcast 3
 * Map/ReplicatedMap:
 * <pre>{@code
 * ServiceFactory<Hz3MapAdapter, AsyncMap<Integer, String>> hz3MapSF =
 *     hz3MapServiceFactory("test-map", HZ3_CLIENT_CONFIG);
 * }</pre>
 * Then use this service factory in a pipeline step
 * {@link GeneralStage#mapUsingService(ServiceFactory, BiFunctionEx)}:
 * <pre>{@code
 * BatchStage<String> mapStage = p.readFrom(TestSources.items(1, 2, 3))
 *  .mapUsingService(
 *    hz3MapSF,
 *    mapUsingIMap(FunctionEx.identity(), (Integer i, String s) -> s)
 *  );
 * mapStage.writeTo(Sinks.list(results));
 * }</pre>
 * And finally, a custom classpath element for the {@code mapUsingService}
 * stage must be set with the Hazelcast 3 client:
 * <pre>{@code
 * List<String> jars = new ArrayList<>();
 * jars.add("hazelcast-3.12.12.jar");
 * jars.add("hazelcast-client-3.12.12.jar");
 * jars.add("hazelcast-3-connector-impl.jar");
 * config.addCustomClasspaths(mapStage.name(), jars);
 * } </pre>
 * The jars must exist in the directory specified by the
 * {@link ClusterProperty#PROCESSOR_CUSTOM_LIB_DIR}
 * directory. This is already set up for the regular zip distribution.
 *
 * @since 5.0
 */
@Beta
public final class Hz3Enrichment {

    private Hz3Enrichment() {
    }

    /**
     * Create a service factory for enrichment using Hazelcast 3 remote map.
     * <p>
     * See the class javadoc for usage.
     */
    @Beta
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
    @Beta
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
    @Beta
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
    @Beta
    public static <K, V, T, R> BiFunctionEx<? super Map<K, V>, ? super T, R> mapUsingIMap(
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (kvMap, t) -> mapFn.apply(t, kvMap.get(lookupKeyFn.apply(t)));
    }

}
