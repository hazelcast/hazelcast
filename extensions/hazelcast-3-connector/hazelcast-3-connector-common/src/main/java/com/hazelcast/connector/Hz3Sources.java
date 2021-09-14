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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;

/**
 * Provides sources to read from Hazelcast 3 cluster.
 * <p>
 * The usage is similar to {@link Sources#remoteMap(String, ClientConfig)}.
 * Because of incompatible APIs the configuration is passed as an XML
 * document in a string - note that the XML configuration must conform
 * to 3.x schema - see <a href="https://www.hazelcast.com/schema/config/">
 *  https://www.hazelcast.com/schema/config/</a>.
 * <p>
 * Usage:
 * <p>
 * <pre>{@code
 * String clientConfig = "...";
 * BatchSource<Map.Entry<Integer, String>> source =
 *   Hz3Sources.remoteMap("test-map", clientConfig);
 * p.readFrom(source);
 * ...
 * } </pre>
 * Additionally, a custom classpath element for the {@code source} stage
 * must be set with the Hazelcast 3 client and the Hazelcast 3 connector:
 * <pre>{@code
 * List<String> jars = new ArrayList<>();
 * jars.add("hazelcast-3.12.12.jar");
 * jars.add("hazelcast-client-3.12.12.jar");
 * jars.add("hazelcast-3-connector-impl.jar");
 * JobConfig config = new JobConfig();
 * config.addCustomClasspaths(source.name(), jars)
 * }</pre>
 * <p>
 * The jars must exist in the directory specified by the
 * {@link ClusterProperty#PROCESSOR_CUSTOM_LIB_DIR}
 * directory. This is already set up for the regular zip distribution.
 *
 * @since 5.0
 */
@Beta
public final class Hz3Sources {

    private Hz3Sources() {
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name in a remote cluster identified by the supplied
     * XML configuration.
     *
     * @param mapName   name of the map
     * @param clientXml client configuration for the remote cluster
     */
    @Beta
    @Nonnull
    public static <K, V> BatchSource<Map.Entry<K, V>> remoteMap(
            @Nonnull String mapName,
            @Nonnull String clientXml
    ) {
        return batchFromProcessor("remoteMapSource(" + mapName + ')',
                ProcessorMetaSupplier.of(readRemoteMapP(mapName, clientXml)));
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig)}.
     */
    @Beta
    @Nonnull
    public static ProcessorSupplier readRemoteMapP(@Nonnull String mapName, @Nonnull String clientXml) {
        return new ReadMapOrCacheP.RemoteProcessorSupplier<>(mapName, clientXml);
    }
}
