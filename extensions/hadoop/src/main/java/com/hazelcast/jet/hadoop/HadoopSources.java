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

package com.hazelcast.jet.hadoop;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.hadoop.impl.SerializableConfiguration;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.hadoop.HadoopProcessors.readHadoopP;

/**
 * Contains factory methods for Apache Hadoop sources.
 *
 * @since Jet 3.0
 */
public final class HadoopSources {

    /**
     * With the new HDFS API, some of the {@link RecordReader}s return the same
     * key/value instances for each record, for example {@link LineRecordReader}.
     * If this property is set to {@code true}, the source makes a copy of each
     * object after applying the {@code projectionFn}. For readers which create
     * a new instance for each record, the source can be configured to not copy
     * the objects for performance.
     * <p>
     * Also if you are using a projection function which doesn't refer to any
     * mutable state from the key or value, then it makes sense to set this
     * property to {@code false} to avoid unnecessary copying.
     * <p>
     * The source copies the objects by serializing and de-serializing them. The
     * objects should be either {@link Writable} or serializable in a way which
     * Jet can serialize/de-serialize.
     * <p>
     * Here is how you can configure the source. Default and always safe value is
     * {@code true}:
     *
     * <pre>{@code
     *     Configuration conf = new Configuration();
     *     conf.setBoolean(HadoopSources.COPY_ON_READ, false);
     *     BatchSource<Entry<K, V>> source = HadoopSources.inputFormat(conf);
     * }</pre>
     */
    public static final String COPY_ON_READ = "jet.source.copyonread";

    /**
     * When reading files from local file system using Hadoop, each processor
     * reads files from its own local file system. If the local file system
     * is shared between members, e.g NFS mounted filesystem, you should
     * configure this property as {@code true}.
     * <p>
     * Here is how you can configure the source. Default value is {@code false}:
     *
     * <pre>{@code
     *     Configuration conf = new Configuration();
     *     conf.setBoolean(HadoopSources.SHARED_LOCAL_FS, true);
     *     BatchSource<Entry<K, V>> source = HadoopSources.inputFormat(conf);
     * }</pre>
     *
     * @since Jet 4.4
     */
    public static final String SHARED_LOCAL_FS = "jet.source.sharedlocalfs";

    /**
     * @since Jet 4.4
     */
    public static final String IGNORE_FILE_NOT_FOUND = "jet.source.ignorefilenotfound";

    private HadoopSources() {
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied projection function.
     * <p>
     * This source splits and balances the input data among Jet {@linkplain
     * Processor processors}, doing its best to achieve data locality. To this
     * end the Jet cluster topology should be aligned with Hadoop's &mdash; on
     * each Hadoop member there should be a Jet member.
     * <p>
     * The processor will use either the new or the old MapReduce API based on
     * the key which stores the {@code InputFormat} configuration. If it's
     * stored under {@value MRJobConfig#INPUT_FORMAT_CLASS_ATTR}, the new API
     * will be used. Otherwise, the old API will be used. If you get the
     * configuration from {@link Job#getConfiguration()}, the new API will be
     * used. Please see {@link #COPY_ON_READ} if you are using the new API.
     * <p>
     * The default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     *
     * @param <K>           key type of the records
     * @param <V>           value type of the records
     * @param <E>           the type of the emitted value
     * @param configuration JobConf for reading files with the appropriate
     *                      input format and path
     * @param projectionFn  function to create output objects from key and value.
     *                      If the projection returns a {@code null} for an item, that item
     *                      will be filtered out
     */
    @Nonnull
    public static <K, V, E> BatchSource<E> inputFormat(
            @Nonnull Configuration configuration,
            @Nonnull BiFunctionEx<K, V, E> projectionFn
    ) {
        return Sources.batchFromProcessor("hdfsSource",
                readHadoopP(SerializableConfiguration.asSerializable(configuration), projectionFn));
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied projection function.
     * <p>
     * This source splits and balances the input data among Jet {@linkplain
     * Processor processors}, doing its best to achieve data locality. To this
     * end the Jet cluster topology should be aligned with Hadoop's &mdash; on
     * each Hadoop member there should be a Jet member.
     * <p>
     * The {@code configureFn} is used to configure the MR Job. The function is
     * run on the coordinator node of the Jet Job, avoiding contacting the server
     * from the machine where the job is submitted.
     * <p>
     * The new MapReduce API will be used.
     * <p>
     * The default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     *
     * @param <K>           key type of the records
     * @param <V>           value type of the records
     * @param <E>           the type of the emitted value
     * @param configureFn   function to configure the MR job
     * @param projectionFn  function to create output objects from key and value.
     *                      If the projection returns a {@code null} for an item, that item
     *                      will be filtered out
     */
    @Nonnull
    public static <K, V, E> BatchSource<E> inputFormat(
            @Nonnull ConsumerEx<Configuration> configureFn,
            @Nonnull BiFunctionEx<K, V, E> projectionFn
    ) {
        return Sources.batchFromProcessor("readHadoop", readHadoopP(null, configureFn, projectionFn));
    }

    /**
     * Convenience for {@link #inputFormat(Configuration, BiFunctionEx)}
     * with {@link java.util.Map.Entry} as its output type.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> inputFormat(@Nonnull Configuration jobConf) {
        return inputFormat(jobConf, (BiFunctionEx<K, V, Entry<K, V>>) Util::entry);
    }
}
