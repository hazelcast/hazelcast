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
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.hadoop.ReadHdfsP.MetaSupplier;
import com.hazelcast.jet.impl.connector.hadoop.SerializableJobConf;
import com.hazelcast.jet.impl.connector.hadoop.WriteHdfsP;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.connector.hadoop.SerializableJobConf.asSerializable;

/**
 * Static utility class with factories of Apache Hadoop HDFS source and sink
 * processors.
 */
public final class HdfsProcessors {

    private HdfsProcessors() {
    }

    /**
     * Returns a meta-supplier of processors for a vertex that reads records
     * from Apache Hadoop HDFS and emits the results of transforming each
     * record with the supplied mapping function.
     * <p>
     * The vertex splits and balances the input data among processors, doing
     * its best to achieve data locality. To this end the Jet cluster topology
     * should be aligned with Hadoop's &mdash; on each Hadoop member there
     * should be a Jet member.
     *
     * @param <K> key type of the records
     * @param <V> value type of the records
     * @param <R> the type of the emitted value

     * @param jobConf JobConf for reading files with the appropriate input format and path
     * @param mapper  mapper which can be used to map the key and value to another value
     */
    @Nonnull
    public static <K, V, R> MetaSupplier<K, V, R> readHdfsP(
            @Nonnull JobConf jobConf, @Nonnull DistributedBiFunction<K, V, R> mapper
    ) {
        return new MetaSupplier<>(asSerializable(jobConf), mapper);
    }

    /**
     * Returns a meta-supplier of processors for a vertex that writes to Apache
     * Hadoop HDFS. It transforms each received item to a key-value pair using
     * the two supplied mapping functions. The type of key and value must
     * conform to the expectations of the output format specified in {@code
     * JobConf}.
     * <p>
     * Each processor instance creates a single file in the output path
     * identified by the member ID and the processor ID. Unlike MapReduce, the
     * data in the files is not sorted by key.
     * <p>
     * The supplied {@code JobConf} must specify an {@code OutputFormat} with
     * a path.
     *
     * @param jobConf        {@code JobConf} used for output format configuration
     * @param extractKeyFn   mapper to map a key to another key
     * @param extractValueFn mapper to map a value to another value
     *
     * @param <E> stream item type
     * @param <K> type of key to write to HDFS
     * @param <V> type of value to write to HDFS
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier writeHdfsP(
            @Nonnull JobConf jobConf,
            @Nonnull DistributedFunction<? super E, K> extractKeyFn,
            @Nonnull DistributedFunction<? super E, V> extractValueFn
    ) {
        return new WriteHdfsP.MetaSupplier<>(SerializableJobConf.asSerializable(jobConf), extractKeyFn, extractValueFn);
    }
}
