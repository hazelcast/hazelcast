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
import com.hazelcast.jet.impl.connector.hadoop.ReadHdfsP;
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
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.HdfsSources#readHdfs(JobConf, DistributedBiFunction)}.
     */
    @Nonnull
    public static <K, V, R> ReadHdfsP.MetaSupplier<K, V, R> readHdfsP(
            @Nonnull JobConf jobConf, @Nonnull DistributedBiFunction<K, V, R> mapper
    ) {
        return new ReadHdfsP.MetaSupplier<>(asSerializable(jobConf), mapper);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.HdfsSinks#writeHdfs(JobConf, DistributedFunction, DistributedFunction)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier writeHdfsP(
            @Nonnull JobConf jobConf,
            @Nonnull DistributedFunction<? super E, K> extractKeyFn,
            @Nonnull DistributedFunction<? super E, V> extractValueFn
    ) {
        return new WriteHdfsP.MetaSupplier<>(asSerializable(jobConf), extractKeyFn, extractValueFn);
    }
}
