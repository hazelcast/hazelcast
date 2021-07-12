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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.impl.ReadHadoopNewApiP;
import com.hazelcast.jet.hadoop.impl.ReadHadoopOldApiP;
import com.hazelcast.jet.hadoop.impl.SerializableConfiguration;
import com.hazelcast.jet.hadoop.impl.WriteHadoopNewApiP;
import com.hazelcast.jet.hadoop.impl.WriteHadoopOldApiP;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;

/**
 * Static utility class with factories of Apache Hadoop Hadoop source and sink
 * processors.
 *
 * @since Jet 3.0
 */
public final class HadoopProcessors {

    private HadoopProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link HadoopSources#inputFormat(Configuration, BiFunctionEx)}.
     */
    @Nonnull
    public static <K, V, R> ProcessorMetaSupplier readHadoopP(
            @Nonnull Configuration configuration,
            @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR) != null) {
            return new ReadHadoopNewApiP.MetaSupplier<>(null, configuration, ConsumerEx.noop(), projectionFn);
        } else {
            return new ReadHadoopOldApiP.MetaSupplier<>((JobConf) configuration, projectionFn);
        }
    }

    /**
     * Returns a supplier of processors for {@link FileSources#files(String)}.
     *
     * The configuration happens via provided {@code configureFn} function on the
     * job coordinator node. This is useful in cases where setting up the
     * configuration requires access to the server and only the cluster members
     * have the access.
     */
    @Nonnull
    public static <K, V, R> ProcessorMetaSupplier readHadoopP(
            @Nullable Permission permission,
            @Nonnull ConsumerEx<Configuration> configureFn,
            @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        return new ReadHadoopNewApiP.MetaSupplier<>(
                permission,
                SerializableConfiguration.asSerializable(new Configuration()),
                configureFn,
                projectionFn
        );
    }

    /**
     * Returns a supplier of processors for
     * {@link HadoopSinks#outputFormat(Configuration, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier writeHadoopP(
            @Nonnull Configuration configuration,
            @Nonnull FunctionEx<? super E, K> extractKeyFn,
            @Nonnull FunctionEx<? super E, V> extractValueFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR) != null) {
            return new WriteHadoopNewApiP.MetaSupplier<>(configuration, extractKeyFn, extractValueFn);
        } else {
            return new WriteHadoopOldApiP.MetaSupplier<>((JobConf) configuration, extractKeyFn, extractValueFn);
        }
    }
}
