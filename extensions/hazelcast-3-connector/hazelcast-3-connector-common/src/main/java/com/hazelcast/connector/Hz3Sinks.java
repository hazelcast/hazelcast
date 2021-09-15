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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

/**
 * Provides sinks to write to Hazelcast 3 cluster.
 * <p>
 * The usage is similar to {@link Sinks#remoteMap(String, ClientConfig)}.
 * Because of incompatible APIs the configuration is passed as an XML
 * document in a string - note that the XML configuration must conform
 * to 3.x schema - see <a href="https://www.hazelcast.com/schema/config/">
 * https://www.hazelcast.com/schema/config/</a>.
 * <p>
 * Usage:
 * <pre>{@code
 * String clientConfig = "...";
 * Sink<Map.Entry<Integer, String>> sink =
 *   Hz3Sinks.map("test-map", HZ3_CLIENT_CONFIG);
 * p.readFrom(source)
 *  .writeTo(sink);
 * } </pre>
 * Additionally, a custom classpath element for the {@code source} stage
 * must be set with the Hazelcast 3 client and the Hazelcast 3 connector:
 * <pre>{@code
 * List<String> jars = new ArrayList<>();
 * jars.add("hazelcast-3.12.12.jar");
 * jars.add("hazelcast-client-3.12.12.jar");
 * jars.add("hazelcast-3-connector-impl.jar");
 * JobConfig config = new JobConfig();
 * config.addCustomClasspaths(sink.name(), jars)
 * }</pre>
 * <p>
 * The jars must exist in the directory specified by the
 * {@link ClusterProperty#PROCESSOR_CUSTOM_LIB_DIR}
 * directory. This is already set up for the regular zip distribution.
 *
 * @since 5.0
 */
@Beta
public final class Hz3Sinks {

    private Hz3Sinks() {
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code IMap} with the specified name.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Beta
    @Nonnull
    public static <K, V> Sink<Map.Entry<K, V>> remoteMap(@Nonnull String mapName, @Nonnull String clientXml) {
        return remoteMap(mapName, Map.Entry::getKey, Map.Entry::getValue, clientXml);
    }

    /**
     * Returns a sink that uses the supplied functions to extract the key
     * and value with which to put to a Hazelcast {@code IMap} with the
     * specified name.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     */
    @Beta
    @Nonnull
    public static <T, K, V> Sink<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull String clientXml) {
        return new SinkImpl<>("mapSink(" + mapName + ')',
                writeRemoteMapP(mapName, toKeyFn, toValueFn, clientXml), toKeyFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#map(String, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    static <T, K, V> ProcessorMetaSupplier writeRemoteMapP(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull String clientXml) {

        return preferLocalParallelismOne(new WriteMapP.Supplier<>(
                clientXml, mapName,
                toKeyFn,
                toValueFn)
        );
    }

}
