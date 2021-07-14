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

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;

/**
 * Sources to connect to Hazelcast 3 cluster
 *
 * TODO documentation how to use
 *
 */
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
    @Nonnull
    public static ProcessorSupplier readRemoteMapP(@Nonnull String mapName, @Nonnull String clientXml) {
        return new ReadMapOrCacheP.RemoteProcessorSupplier<>(mapName, clientXml);
    }
}
