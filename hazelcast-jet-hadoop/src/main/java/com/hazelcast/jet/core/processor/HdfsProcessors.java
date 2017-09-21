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

package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.hadoop.ReadHdfsP;
import com.hazelcast.jet.impl.connector.hadoop.ReadHdfsP.MetaSupplier;
import com.hazelcast.jet.impl.connector.hadoop.SerializableJobConf;
import com.hazelcast.jet.impl.connector.hadoop.WriteHdfsP;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.impl.connector.hadoop.SerializableJobConf.asSerializable;

/**
 * Static utility class with factories of Apache Hadoop HDFS source and sink
 * processors.
 */
public final class HdfsProcessors {

    private HdfsProcessors() {
    }

    /**
     * Convenience for {@link #readHdfs(JobConf, DistributedBiFunction)}
     * emitting output as a {@link java.util.Map.Entry}.
     */
    @Nonnull
    public static <K, V> ReadHdfsP.MetaSupplier<K, V, Entry<K, V>> readHdfs(@Nonnull JobConf jobConf) {
        return readHdfs(jobConf, Util::entry);
    }

    /**
     * A meta-supplier of processor which reads and emits records from Apache
     * Hadoop HDFS.
     *
     * The input according to the given {@code InputFormat} is split among the
     * processor instances and each processor instance is responsible for
     * reading a part of the input. The records are emitted as {@code
     * Map.Entry<K,V>} by default, but this can also be transformed to another
     * type using an optional {@code mapper}.
     *
     * Jet cluster should be run on the same machines as the Apache Hadoop
     * cluster for best read performance. If the hosts are aligned, each
     * processor instance will try to read as much local data as possible. A
     * heuristic algorithm is used to assign replicated blocks across the
     * cluster to ensure a well-balanced work distribution between processor
     * instances.
     *
     * @param <K> key type of the records
     * @param <V> value type of the records
     * @param <R> the type of the emitted value

     * @param jobConf JobConf for reading files with the appropriate input format and path
     * @param mapper  mapper which can be used to map the key and value to another value
     */
    @Nonnull
    public static <K, V, R> MetaSupplier<K, V, R> readHdfs(
            @Nonnull JobConf jobConf, @Nonnull DistributedBiFunction<K, V, R> mapper
    ) {
        return new MetaSupplier<>(asSerializable(jobConf), mapper);
    }

    /**
     * Convenience for {@link #writeHdfs(JobConf, DistributedFunction,
     * DistributedFunction)} with {@code identity()} mapping functions.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeHdfs(@Nonnull JobConf jobConf) {
        return writeHdfs(jobConf, identity(), identity());
    }

    /**
     * Returns a meta-supplier of processor that writes to Apache Hadoop HDFS.
     * The processor expects items of type {@code Map.Entry<K,V>} on input and
     * takes optional mappers for converting the key and the value to types
     * required by the output format. For example, the mappers can be used to
     * map the keys and the values to their {@code Writable} equivalents.
     *
     * Each processor instance creates a single file in the output path identified by
     * the member ID and the processor ID. Unlike MapReduce, the output files
     * are not sorted by key.
     *
     * The supplied {@code JobConf} must specify an {@code OutputFormat} with
     * a path.
     *
     * @param jobConf     {@code JobConf} used for output format configuration
     * @param keyMapper   mapper to map a key to another key
     * @param valueMapper mapper to map a value to another value
     *
     * @param <K>         input key type
     * @param <KM>        the type of the key after mapping
     * @param <V>         input value type
     * @param <VM>        the type of the value after mapping
     */
    @Nonnull
    public static <K, KM, V, VM> ProcessorMetaSupplier writeHdfs(
            @Nonnull JobConf jobConf,
            @Nonnull DistributedFunction<K, KM> keyMapper,
            @Nonnull DistributedFunction<V, VM> valueMapper
    ) {
        return new WriteHdfsP.MetaSupplier<>(SerializableJobConf.asSerializable(jobConf), keyMapper, valueMapper);
    }

}
