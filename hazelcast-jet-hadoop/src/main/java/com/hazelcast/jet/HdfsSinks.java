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

package com.hazelcast.jet;

import com.hazelcast.jet.core.processor.HdfsProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Factories of Apache Hadoop HDFS sinks.
 */
public final class HdfsSinks {

    private HdfsSinks() {
    }

    /**
     * Returns a sink that writes to Apache Hadoop HDFS. It transforms each
     * received item to a key-value pair using the two supplied mapping
     * functions. The type of key and value must conform to the expectations
     * of the output format specified in {@code JobConf}.
     * <p>
     * The sink creates a number of files in the output path, identified by the
     * cluster member ID and the {@link com.hazelcast.jet.core.Processor
     * processor} ID. Unlike MapReduce, the data in the files is not sorted by
     * key.
     * <p>
     * The supplied {@code JobConf} must specify an {@code OutputFormat} with
     * a path.
     *
     * @param jobConf     {@code JobConf} used for output format configuration
     * @param extractKeyF   mapper to map a key to another key
     * @param extractValueF mapper to map a value to another value
     *
     * @param <E> stream item type
     * @param <K> type of key to write to HDFS
     * @param <V> type of value to write to HDFS
     */
    @Nonnull
    public static <E, K, V> Sink<E> writeHdfs(
            @Nonnull JobConf jobConf,
            @Nonnull DistributedFunction<? super E, K> extractKeyF,
            @Nonnull DistributedFunction<? super E, V> extractValueF
    ) {
        return Sinks.fromProcessor("writeHdfs", HdfsProcessors.writeHdfsP(jobConf, extractKeyF, extractValueF));
    }

    /**
     * Convenience for {@link #writeHdfs(JobConf, DistributedFunction,
     * DistributedFunction)} which expects {@code Map.Entry<K, V>} as
     * input and extracts its key and value parts to be written to HDFS.
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> writeHdfs(@Nonnull JobConf jobConf) {
        return writeHdfs(jobConf, Entry::getKey, Entry::getValue);
    }

}
