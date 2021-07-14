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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

/**
 * Sinks to connect to Hazelcast 3 cluster
 */
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
    @Nonnull
    public static <K, V> Sink<Map.Entry<K, V>> map(@Nonnull String mapName, @Nonnull String clientXml) {
        return map(mapName, Map.Entry::getKey, Map.Entry::getValue, clientXml);
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
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <T, K, V> Sink<T> map(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull String clientXml) {
        return new SinkImpl<>("mapSink(" + mapName + ')',
                writeMapP(mapName, toKeyFn, toValueFn, clientXml), toKeyFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#map(String, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    static <T, K, V> ProcessorMetaSupplier writeMapP(
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
