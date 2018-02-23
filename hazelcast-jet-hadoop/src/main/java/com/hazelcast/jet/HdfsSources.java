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

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.connector.hadoop.ReadHdfsP.MetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.connector.hadoop.SerializableJobConf.asSerializable;

/**
 * Contains factory methods for Apache Hadoop HDFS sources.
 */
public final class HdfsSources {

    private HdfsSources() {
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied mapping function.
     * <p>
     * This source splits and balances the input data among Jet {@link
     * com.hazelcast.jet.core.Processor processors}, doing its best to achieve
     * data locality. To this end the Jet cluster topology should be aligned
     * with Hadoop's &mdash; on each Hadoop member there should be a Jet
     * member.
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     *
     * @param <K> key type of the records
     * @param <V> value type of the records
     * @param <E> the type of the emitted value

     * @param jobConf JobConf for reading files with the appropriate input format and path
     * @param projectionFn function to create output objects from key and value.
     *                     If the projection returns a {@code null} for an item, that item
     *                     will be filtered out
     */
    @Nonnull
    public static <K, V, E> BatchSource<E> hdfs(
            @Nonnull JobConf jobConf,
            @Nonnull DistributedBiFunction<K, V, E> projectionFn
    ) {
        return Sources.batchFromProcessor("readHdfs", new MetaSupplier<>(asSerializable(jobConf), projectionFn));
    }

    /**
     * Convenience for {@link #hdfs(JobConf, DistributedBiFunction)}
     * with {@link java.util.Map.Entry} as its output type.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> hdfs(@Nonnull JobConf jobConf) {
        return hdfs(jobConf, (DistributedBiFunction<K, V, Entry<K, V>>) Util::entry);
    }
}
