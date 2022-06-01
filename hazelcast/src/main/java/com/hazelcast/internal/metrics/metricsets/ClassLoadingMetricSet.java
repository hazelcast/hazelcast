/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.nio.ClassLoaderUtil;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_FULL_METRIC_LOADED_CLASSES_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_FULL_METRIC_TOTAL_LOADED_CLASSES_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_FULL_METRIC_UNLOADED_CLASSES_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_JET_EXTENSIONS_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BOOLEAN;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A Metric set for exposing {@link java.lang.management.ClassLoadingMXBean} metrics and Jet extensions' availability.
 */
public final class ClassLoadingMetricSet {

    public static final Map<String, String> JET_EXTENSION_TO_CLASS_MAPPING = new HashMap<>();

    static {
        JET_EXTENSION_TO_CLASS_MAPPING.put("kafka", "com.hazelcast.jet.kafka.KafkaSinks");

        JET_EXTENSION_TO_CLASS_MAPPING.put("csv", "com.hazelcast.jet.csv.impl.CsvReadFileFnProvider");
        JET_EXTENSION_TO_CLASS_MAPPING.put("avro", "com.hazelcast.jet.avro.AvroSources");
        JET_EXTENSION_TO_CLASS_MAPPING.put("parquet", "org.apache.parquet.avro.AvroParquetInputFormat");

        JET_EXTENSION_TO_CLASS_MAPPING.put("hadoop", "com.hazelcast.jet.hadoop.HadoopProcessors");
        JET_EXTENSION_TO_CLASS_MAPPING.put("files-s3", "org.apache.hadoop.fs.s3a.S3A");
        JET_EXTENSION_TO_CLASS_MAPPING.put("files-gcs", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        JET_EXTENSION_TO_CLASS_MAPPING.put("files-azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    }

    private ClassLoadingMetricSet() {
    }

    /**
     * Registers all the metrics in this metric pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");

        ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();

        metricsRegistry.registerStaticProbe(mxBean, CLASSLOADING_FULL_METRIC_LOADED_CLASSES_COUNT, MANDATORY,
                ClassLoadingMXBean::getLoadedClassCount);

        metricsRegistry.registerStaticProbe(mxBean, CLASSLOADING_FULL_METRIC_TOTAL_LOADED_CLASSES_COUNT, MANDATORY,
                ClassLoadingMXBean::getTotalLoadedClassCount);

        metricsRegistry.registerStaticProbe(mxBean, CLASSLOADING_FULL_METRIC_UNLOADED_CLASSES_COUNT, MANDATORY,
                ClassLoadingMXBean::getUnloadedClassCount);

        MetricDescriptor extensionsJetDescriptor = metricsRegistry.newMetricDescriptor()
                .withPrefix(CLASSLOADING_JET_EXTENSIONS_PREFIX);
        JET_EXTENSION_TO_CLASS_MAPPING.forEach((extensionName, fqdn) -> metricsRegistry.registerStaticProbe(
                "classpath", extensionsJetDescriptor, extensionName, INFO, BOOLEAN,
                (LongProbeFunction<?>) ignored -> ClassLoaderUtil.isClassDefined(fqdn) ? 1L : 0L)
        );
    }
}
